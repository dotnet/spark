// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Text;
using Razorvine.Pickle.Objects;

namespace Razorvine.Pickle
{

/// <summary>
/// Unpickles an object graph from a pickle data inputstream. Supports all pickle protocol versions.
/// Maps the python objects on the corresponding java equivalents or similar types.
/// This class is NOT threadsafe! (Don't use the same unpickler from different threads)
/// See the README.txt for a table with the type mappings.
/// </summary>
[SuppressMessage("ReSharper", "InconsistentNaming")]
[SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
[SuppressMessage("ReSharper", "InvertIf")]
internal class Unpickler : IDisposable {

	protected const int HIGHEST_PROTOCOL = 4;

	protected readonly IDictionary<int, object> memo;
	protected UnpickleStack stack;
	protected Stream input;
	protected static readonly IDictionary<string, IObjectConstructor> objectConstructors = CreateObjectConstructorsDictionary();
	protected static readonly object NO_RETURN_VALUE = new object();
    private static readonly string[] quoteStrings = new [] { "\"", "'" };
    private static readonly object boxedFalse = false;
    private static readonly object boxedTrue = true;
    private byte[] byteBuffer = new byte[sizeof(long)]; // at least large enough for any primitive being deserialized
    private Dictionary<StringPair, string> concatenatedModuleNames;

    private readonly struct StringPair : IEquatable<StringPair>
    {
        public readonly string Item1, Item2;
        public StringPair(string item1, string item2) { Item1 = item1; Item2 = item2; }
        public bool Equals(StringPair other) => Item1 == other.Item1 && Item2 == other.Item2;
        public override bool Equals(object obj) => obj is StringPair sp && Equals(sp);
        public override int GetHashCode() => Item1.GetHashCode() ^ Item2.GetHashCode();
    }

    private static Dictionary<string, IObjectConstructor> CreateObjectConstructorsDictionary()
    {
        return new Dictionary<string, IObjectConstructor>(15)
		{
			["__builtin__.complex"] = new AnyClassConstructor(typeof(ComplexNumber)),
			["builtins.complex"] = new AnyClassConstructor(typeof(ComplexNumber)),
			["array.array"] = new ArrayConstructor(),
			["array._array_reconstructor"] = new ArrayConstructor(),
			["__builtin__.bytearray"] = new ByteArrayConstructor(),
			["builtins.bytearray"] = new ByteArrayConstructor(),
			["__builtin__.bytes"] = new ByteArrayConstructor(),
			["__builtin__.set"] = new SetConstructor(),
			["builtins.set"] = new SetConstructor(),
			["datetime.datetime"] = new DateTimeConstructor(DateTimeConstructor.PythonType.DateTime),
			["datetime.time"] = new DateTimeConstructor(DateTimeConstructor.PythonType.Time),
			["datetime.date"] = new DateTimeConstructor(DateTimeConstructor.PythonType.Date),
			["datetime.timedelta"] = new DateTimeConstructor(DateTimeConstructor.PythonType.TimeDelta),
			["decimal.Decimal"] = new DecimalConstructor(),
			["_codecs.encode"] = new ByteArrayConstructor()
		};
		// we're lucky, the bytearray constructor is also able to mimic codecs.encode()
	}

	/**
	 * Create an unpickler.
	 */
	public Unpickler() {
		memo = new Dictionary<int, object>();
	}

	/**
	 * Register additional object constructors for custom classes.
	 */
	public static void registerConstructor(string module, string classname, IObjectConstructor constructor) {
		objectConstructors[module + "." + classname]=constructor;
	}

	/**
	 * Read a pickled object representation from the given input stream.
	 * 
	 * @return the reconstituted object hierarchy specified in the file.
	 */
	public object load(Stream stream) {
		input = stream;
		stack = new UnpickleStack();
		while (true) {
			byte key = PickleUtils.readbyte(input);
			object value = dispatch(key);
			if(value != NO_RETURN_VALUE)
				return value;
		}
	}

	/**
	 * Read a pickled object representation from the given pickle data bytes.
	 * 
	 * @return the reconstituted object hierarchy specified in the file.
	 */
	public object loads(byte[] pickledata) {
		return load(new MemoryStream(pickledata));
	}

	/**
	 * Close the unpickler and frees the resources such as the unpickle stack and memo table.
	 */
	public void close() {
		stack?.clear();
		memo?.Clear();
		input?.Close();
	}

	/**
	 * Process a single pickle stream opcode.
	 */
	protected object dispatch(short key) {
		switch (key) {
		case Opcodes.MARK:
			load_mark();
			break;
		case Opcodes.STOP:
			object value = stack.pop();
			stack.clear();
			return value;		// final result value
		case Opcodes.POP:
			load_pop();
			break;
		case Opcodes.POP_MARK:
			load_pop_mark();
			break;
		case Opcodes.DUP:
			load_dup();
			break;
		case Opcodes.FLOAT:
			load_float();
			break;
		case Opcodes.INT:
			load_int();
			break;
		case Opcodes.BININT:
			load_binint();
			break;
		case Opcodes.BININT1:
			load_binint1();
			break;
		case Opcodes.LONG:
			load_long();
			break;
		case Opcodes.BININT2:
			load_binint2();
			break;
		case Opcodes.NONE:
			load_none();
			break;
		case Opcodes.PERSID:
			load_persid();
			break;
		case Opcodes.BINPERSID:
			load_binpersid();
			break;
		case Opcodes.REDUCE:
			load_reduce();
			break;
		case Opcodes.STRING:
			load_string();
			break;
		case Opcodes.BINSTRING:
			load_binstring();
			break;
		case Opcodes.SHORT_BINSTRING:
			load_short_binstring();
			break;
		case Opcodes.UNICODE:
			load_unicode();
			break;
		case Opcodes.BINUNICODE:
			load_binunicode();
			break;
		case Opcodes.APPEND:
			load_append();
			break;
		case Opcodes.BUILD:
			load_build();
			break;
		case Opcodes.GLOBAL:
			load_global();
			break;
		case Opcodes.DICT:
			load_dict();
			break;
		case Opcodes.EMPTY_DICT:
			load_empty_dictionary();
			break;
		case Opcodes.APPENDS:
			load_appends();
			break;
		case Opcodes.GET:
			load_get();
			break;
		case Opcodes.BINGET:
			load_binget();
			break;
		case Opcodes.INST:
			load_inst();
			break;
		case Opcodes.LONG_BINGET:
			load_long_binget();
			break;
		case Opcodes.LIST:
			load_list();
			break;
		case Opcodes.EMPTY_LIST:
			load_empty_list();
			break;
		case Opcodes.OBJ:
			load_obj();
			break;
		case Opcodes.PUT:
			load_put();
			break;
		case Opcodes.BINPUT:
			load_binput();
			break;
		case Opcodes.LONG_BINPUT:
			load_long_binput();
			break;
		case Opcodes.SETITEM:
			load_setitem();
			break;
		case Opcodes.TUPLE:
			load_tuple();
			break;
		case Opcodes.EMPTY_TUPLE:
			load_empty_tuple();
			break;
		case Opcodes.SETITEMS:
			load_setitems();
			break;
		case Opcodes.BINFLOAT:
			load_binfloat();
			break;

		// protocol 2
		case Opcodes.PROTO:
			load_proto();
			break;
		case Opcodes.NEWOBJ:
			load_newobj();
			break;
		case Opcodes.EXT1:
		case Opcodes.EXT2:
		case Opcodes.EXT4:
			throw new PickleException("Unimplemented opcode EXT1/EXT2/EXT4 encountered. Don't use extension codes when pickling via copyreg.add_extension() to avoid this error.");
		case Opcodes.TUPLE1:
			load_tuple1();
			break;
		case Opcodes.TUPLE2:
			load_tuple2();
			break;
		case Opcodes.TUPLE3:
			load_tuple3();
			break;
		case Opcodes.NEWTRUE:
			load_true();
			break;
		case Opcodes.NEWFALSE:
			load_false();
			break;
		case Opcodes.LONG1:
			load_long1();
			break;
		case Opcodes.LONG4:
			load_long4();
			break;

		// Protocol 3 (Python 3.x)
		case Opcodes.BINBYTES:
			load_binbytes();
			break;
		case Opcodes.SHORT_BINBYTES:
			load_short_binbytes();
			break;
			
		// Protocol 4 (Python 3.4+)
		case Opcodes.BINUNICODE8:
			load_binunicode8();
			break;
		case Opcodes.SHORT_BINUNICODE:
			load_short_binunicode();
			break;
		case Opcodes.BINBYTES8:
			load_binbytes8();
			break;
		case Opcodes.EMPTY_SET:
			load_empty_set();
			break;
		case Opcodes.ADDITEMS:
			load_additems();
			break;
		case Opcodes.FROZENSET:
			load_frozenset();
			break;
		case Opcodes.MEMOIZE:
			load_memoize();
			break;
		case Opcodes.FRAME:
			load_frame();
			break;
		case Opcodes.NEWOBJ_EX:
			load_newobj_ex();
			break;
		case Opcodes.STACK_GLOBAL:
			load_stack_global();
			break;

		default:
			throw new InvalidOpcodeException("invalid pickle opcode: " + key);
		}
		
		return NO_RETURN_VALUE;
	}

	private void load_build() {
		object args=stack.pop();
		object target=stack.peek();
		object[] arguments={args};
		Type[] argumentTypes={args.GetType()};
		
		// call the __setstate__ method with the given arguments
		try {
			MethodInfo setStateMethod=target.GetType().GetMethod("__setstate__", argumentTypes);
			if(setStateMethod==null) {
				throw new PickleException($"no __setstate__() found in type {target.GetType()} with argument type {args.GetType()}");
			}
			setStateMethod.Invoke(target, arguments);
		} catch(Exception e) {
			throw new PickleException("failed to __setstate__()",e);
		}
	}

	private void load_proto() {
		byte proto = PickleUtils.readbyte(input);
		if (proto > HIGHEST_PROTOCOL)
			throw new PickleException("unsupported pickle protocol: " + proto);
	}

	private void load_none() {
		stack.add(null);
	}

	private void load_false() {
		stack.add(boxedFalse);
	}

	private void load_true() {
		stack.add(boxedTrue);
	}

    private void load_int() {
        int len = PickleUtils.readline_into(input, ref byteBuffer, includeLF: true);
        object val;
        if (len == 3 && byteBuffer[2] == (byte)'\n' && byteBuffer[0] == (byte)'0') {
            if (byteBuffer[1] == (byte)'0') {
                load_false();
                return;
            }
            else if (byteBuffer[1] == (byte)'1') {
                load_true();
                return;
            }
        }

        len--;
        if (len > 0 && Utf8Parser.TryParse(byteBuffer.AsSpan(0, len), out int intNumber, out int bytesConsumed) && bytesConsumed == len) {
            val = intNumber;
        }
        else if (len > 0 && Utf8Parser.TryParse(byteBuffer.AsSpan(0, len), out long longNumber, out bytesConsumed) && bytesConsumed == len) {
            val = longNumber;
        }
        else {
            val = long.Parse(PickleUtils.rawStringFromBytes(byteBuffer.AsSpan(0, len)));
            Debug.Fail("long.Parse should have thrown.");
        }

        stack.add(val);
    }

	private void load_binint()  {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(int));
		int integer = BinaryPrimitives.ReadInt32LittleEndian(byteBuffer);
		stack.add(integer);
	}

	private void load_binint1() {
		stack.add((int)PickleUtils.readbyte(input));
	}

	private void load_binint2() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(short));
		int integer = BinaryPrimitives.ReadUInt16LittleEndian(byteBuffer);
		stack.add(integer);
	}

	private void load_long() {
		string val = PickleUtils.readline(input);
		if (val.EndsWith("L")) {
			val = val.Substring(0, val.Length - 1);
		}
		long longvalue;
		if(long.TryParse(val, out longvalue)) {
			stack.add(longvalue);
		} else {
			throw new PickleException("long too large in load_long (need BigInt)");
		}
	}

	private void load_long1() {
		byte n = PickleUtils.readbyte(input);
		var data = PickleUtils.readbytes(input, n);
		stack.add(PickleUtils.decode_long(data));
	}

	private void load_long4() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(int));
		int n = BinaryPrimitives.ReadInt32LittleEndian(byteBuffer);
		var data = PickleUtils.readbytes(input, n);
		stack.add(PickleUtils.decode_long(data));
	}

	private void load_float() {
        int len = PickleUtils.readline_into(input, ref byteBuffer, includeLF: true);
        ReadOnlySpan<byte> bytes = byteBuffer.AsSpan(0, len);
        if (!Utf8Parser.TryParse(bytes, out double d, out int bytesConsumed) || !PickleUtils.IsWhitespace(bytes.Slice(bytesConsumed)))
        {
            throw new FormatException();
        }
		stack.add(d);
	}

	private void load_binfloat() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(long));
        double val = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64BigEndian(byteBuffer));
		stack.add(val);
	}

	private void load_string() {
		string rep = PickleUtils.readline(input);
		bool quotesOk = false;
		foreach (string q in quoteStrings) // double or single quote
		{
			if (rep.StartsWith(q)) {
				if (!rep.EndsWith(q)) {
					throw new PickleException("insecure string pickle");
				}
				rep = rep.Substring(1, rep.Length - 2); // strip quotes
				quotesOk = true;
				break;
			}
		}

		if (!quotesOk)
			throw new PickleException("insecure string pickle");

		stack.add(PickleUtils.decode_escaped(rep));
	}

	private void load_binstring() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(int));
		int len = BinaryPrimitives.ReadInt32LittleEndian(byteBuffer);
        EnsureByteBufferLength(len);
        PickleUtils.readbytes_into(input, byteBuffer, 0, len);
		stack.add(PickleUtils.rawStringFromBytes(byteBuffer.AsSpan(0, len)));
	}

	private void load_binbytes() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(int));
		int len = BinaryPrimitives.ReadInt32LittleEndian(byteBuffer);
		stack.add(PickleUtils.readbytes(input, len));
	}

	private void load_binbytes8() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(long));
        long len = BinaryPrimitives.ReadInt64LittleEndian(byteBuffer);
		stack.add(PickleUtils.readbytes(input, len));
	}

	private void load_unicode() {
		string str=PickleUtils.decode_unicode_escaped(PickleUtils.readline(input));
		stack.add(str);
	}

	private void load_binunicode() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(int));
		int len = BinaryPrimitives.ReadInt32LittleEndian(byteBuffer);
		var data = PickleUtils.readbytes(input, len);
		stack.add(Encoding.UTF8.GetString(data));
	}

    private void EnsureByteBufferLength(long len) {
        if (len > byteBuffer.Length)
        {
            byteBuffer = new byte[Math.Max(len, byteBuffer.Length * 2)];
        }
    }

	private unsafe void load_binunicode8() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(long));
        long len = BinaryPrimitives.ReadInt64LittleEndian(byteBuffer);
        EnsureByteBufferLength(len);
        PickleUtils.readbytes_into(input, byteBuffer, 0, len);
        stack.add(Encoding.UTF8.GetString(byteBuffer, 0, (int)len));
	}

	private void load_short_binunicode() {
		int len = PickleUtils.readbyte(input);
		var data = PickleUtils.readbytes(input, len);
		stack.add(Encoding.UTF8.GetString(data));
	}

	private void load_short_binstring() {
		byte len = PickleUtils.readbyte(input);
        EnsureByteBufferLength(len);
		PickleUtils.readbytes_into(input, byteBuffer, 0, len);
		stack.add(PickleUtils.rawStringFromBytes(byteBuffer.AsSpan(0, len)));
	}

	private void load_short_binbytes() {
		byte len = PickleUtils.readbyte(input);
		stack.add(PickleUtils.readbytes(input, len));
	}

	private void load_tuple() {
		stack.add(stack.pop_all_since_marker_as_array());
	}

	private void load_empty_tuple() {
		stack.add(Array.Empty<object>());
	}

	private void load_tuple1() {
		stack.add(new [] { stack.pop() });
	}

	private void load_tuple2() {
		object o2 = stack.pop();
		object o1 = stack.pop();
		stack.add(new [] { o1, o2 });
	}

	private void load_tuple3() {
		object o3 = stack.pop();
		object o2 = stack.pop();
		object o1 = stack.pop();
		stack.add(new [] { o1, o2, o3 });
	}

	private void load_empty_list() {
		stack.add(new ArrayList(5));
	}

	private void load_empty_dictionary() {
		stack.add(new Hashtable(5));
	}

	private void load_empty_set() {
		stack.add(new HashSet<object>());
	}

	private void load_list() {
		ArrayList top = stack.pop_all_since_marker();
		stack.add(top); // simply add the top items as a list to the stack again
	}

	private void load_dict() {
		object[] top = stack.pop_all_since_marker_as_array();
		Hashtable map=new Hashtable(top.Length);
		for (int i = 0; i < top.Length; i += 2) {
			object key = top[i];
			object value = top[i+1];
			map[key]=value;
		}
		stack.add(map);
	}

	private void load_frozenset() {
		object[] top = stack.pop_all_since_marker_as_array();
		var set = new HashSet<object>();
		foreach(var element in top)
			set.Add(element);
		stack.add(set);
	}

	private void load_additems() {
		object[] top = stack.pop_all_since_marker_as_array();
		var set = (HashSet<object>) stack.pop();
		foreach(object item in top)
			set.Add(item);
		stack.add(set);
	}

	private void load_global() {
        int stringLen = PickleUtils.readline_into(input, ref byteBuffer);
        string module = Encoding.UTF8.GetString(byteBuffer, 0, stringLen);

        stringLen = PickleUtils.readline_into(input, ref byteBuffer);
        string name = Encoding.UTF8.GetString(byteBuffer, 0, stringLen);

        load_global_sub(module, name);
	}

	private void load_stack_global() {
		string name = (string) stack.pop();
		string module = (string) stack.pop();
		load_global_sub(module, name);
	}

    private string GetModuleNameKey(string module, string name) {
        if (concatenatedModuleNames == null) {
            concatenatedModuleNames = new Dictionary<StringPair, string>();
        }

        var sp = new StringPair(module, name);
        if (!concatenatedModuleNames.TryGetValue(sp, out string key)) {
            key = module + "." + name;
            concatenatedModuleNames.Add(sp, key);
        }

        return key;
    }

	private void load_global_sub(string module, string name) {
        if (!objectConstructors.TryGetValue(GetModuleNameKey(module, name), out IObjectConstructor constructor)) {
            switch (module) {
                // check if it is an exception
                case "exceptions":
                    // python 2.x
                    constructor = new ExceptionConstructor(typeof(PythonException), module, name);
                    break;
                case "builtins":
                case "__builtin__":
                    if (name.EndsWith("Error") || name.EndsWith("Warning") || name.EndsWith("Exception")
                        || name == "GeneratorExit" || name == "KeyboardInterrupt"
                        || name == "StopIteration" || name == "SystemExit") {
                        // it's a python 3.x exception
                        constructor = new ExceptionConstructor(typeof(PythonException), module, name);
                    }
                    else {
                        // return a dictionary with the class's properties
                        constructor = new ClassDictConstructor(module, name);
                    }

                    break;
                default:
                    constructor = new ClassDictConstructor(module, name);
                    break;
            }
        }
		stack.add(constructor);		
	}

	private void load_pop() {
		stack.pop();
	}

	private void load_pop_mark() {
		object o;
		do {
			o = stack.pop();
		} while (o != stack.MARKER);
		stack.trim();
	}

	private void load_dup() {
		stack.add(stack.peek());
	}

	private void load_get() {
		int i = int.Parse(PickleUtils.readline(input));
		if(!memo.ContainsKey(i)) throw new PickleException("invalid memo key");
		stack.add(memo[i]);
	}

	private void load_binget() {
		byte i = PickleUtils.readbyte(input);
		if(!memo.ContainsKey(i)) throw new PickleException("invalid memo key");
		stack.add(memo[i]);
	}

	private void load_long_binget() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(int));
		int i = BinaryPrimitives.ReadInt32LittleEndian(byteBuffer);
		if(!memo.ContainsKey(i)) throw new PickleException("invalid memo key");
		stack.add(memo[i]);
	}

	private void load_put() {
		int i = int.Parse(PickleUtils.readline(input));
		memo[i]=stack.peek();
	}

	private void load_binput() {
		byte i = PickleUtils.readbyte(input);
		memo[i]=stack.peek();
	}

	private void load_memoize() {
		memo[memo.Count]=stack.peek();
	}

	private void load_long_binput() {
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(int));
		int i = BinaryPrimitives.ReadInt32LittleEndian(byteBuffer);
		memo[i]=stack.peek();
	}

	private void load_append() {
		object value = stack.pop();
		ArrayList list = (ArrayList) stack.peek();
		list.Add(value);
	}

	private void load_appends() {
		object[] top = stack.pop_all_since_marker_as_array();
		ArrayList list = (ArrayList) stack.peek();
        for (int i = 0; i < top.Length; i++) {
            list.Add(top[i]);
        }
	}

	private void load_setitem() {
		object value = stack.pop();
		object key = stack.pop();
		Hashtable dict=(Hashtable)stack.peek();
		dict[key]=value;
	}

	private void load_setitems() {
		var newitems=new List<KeyValuePair<object,object>>();
		object value = stack.pop();
		while (value != stack.MARKER) {
			object key = stack.pop();
			newitems.Add(new KeyValuePair<object,object>(key,value));
			value = stack.pop();
		}
		
		Hashtable dict=(Hashtable)stack.peek();
		foreach(var item in newitems) {
			dict[item.Key]=item.Value;
		}
	}

	private void load_mark() {
		stack.add_mark();
	}

	private void load_reduce() {
		var args = (object[]) stack.pop();
		IObjectConstructor constructor = (IObjectConstructor) stack.pop();
		stack.add(constructor.construct(args));
	}

	private void load_newobj() {
		load_reduce(); // we just do the same as class(*args) instead of class.__new__(class,*args)
	}

	private void load_newobj_ex() {
		Hashtable kwargs = (Hashtable) stack.pop();
		var args = (object[]) stack.pop();
		IObjectConstructor constructor = (IObjectConstructor) stack.pop();
		if(kwargs.Count==0)
			stack.add(constructor.construct(args));
		else
			throw new PickleException("newobj_ex with keyword arguments not supported");
	}

	private void load_frame() {
		// for now we simply skip the frame opcode and its length
        PickleUtils.readbytes_into(input, byteBuffer, 0, sizeof(long));
	}

	private void load_persid() {
		// the persistent id is taken from the argument
		string pid = PickleUtils.readline(input);
		stack.add(persistentLoad(pid));
	}

	private void load_binpersid() {
		// the persistent id is taken from the stack
		string pid = stack.pop().ToString();
		stack.add(persistentLoad(pid));
	}

	private void load_obj() {
		object[] popped = stack.pop_all_since_marker_as_array();

        object[] args;
        if (popped.Length > 1)
        {
            args = new object[popped.Length - 1];
            Array.Copy(popped, 1, args, 0, args.Length);
        }
        else
        {
            args = Array.Empty<object>();
        }

		stack.add(((IObjectConstructor)popped[0]).construct(args));
	}

	private void load_inst() {
        int stringLen = PickleUtils.readline_into(input, ref byteBuffer);
        string module = Encoding.UTF8.GetString(byteBuffer, 0, stringLen);

        stringLen = PickleUtils.readline_into(input, ref byteBuffer);
        string classname = Encoding.UTF8.GetString(byteBuffer, 0, stringLen);

		object[] args = stack.pop_all_since_marker_as_array();

		if(!objectConstructors.TryGetValue(GetModuleNameKey(module, classname), out IObjectConstructor constructor)) {
			constructor = new ClassDictConstructor(module, classname);
            args = Array.Empty<object>(); // classdict doesn't have constructor args... so we may lose info here, hmm.
		}
		stack.add(constructor.construct(args));
	}
	
	protected virtual object persistentLoad(string pid)
	{
		throw new PickleException("A load persistent id instruction was encountered, but no persistentLoad function was specified. (implement it in custom Unpickler subclass)");
	}
	
	public void Dispose()
	{
		close();
	}
}

}
