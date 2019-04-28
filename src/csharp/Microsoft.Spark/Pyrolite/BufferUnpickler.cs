// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text;
using Razorvine.Pickle.Objects;

namespace Razorvine.Pickle
{
    /// <summary>
    /// Unpickles an object graph from a pickle data memory buffer. Supports all pickle protocol versions.
    /// Maps the python objects on the corresponding java equivalents or similar types.
    /// This class is NOT threadsafe! (Don't use the same unpickler from different threads)
    /// See the README.txt for a table with the type mappings.
    /// </summary>
    [SuppressMessage("ReSharper", "InconsistentNaming")]
    [SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
    [SuppressMessage("ReSharper", "InvertIf")]
    internal class BufferUnpickler
    {
        private const int HIGHEST_PROTOCOL = 4;

        private static readonly IDictionary<string, IObjectConstructor> objectConstructors = CreateObjectConstructorsDictionary();
        private static readonly string[] quoteStrings = new[] { "\"", "'" };
        private static readonly object boxedFalse = false;
        private static readonly object boxedTrue = true;

        private readonly IDictionary<int, object> memo;
        private UnpickleStack stack;
        private Dictionary<StringPair, string> concatenatedModuleNames;

        public BufferUnpickler() => memo = new Dictionary<int, object>();

        /// <summary>
        /// Register additional object constructors for custom classes.
        /// </summary>
        public static void registerConstructor(string module, string classname, IObjectConstructor constructor) 
            => objectConstructors[module + "." + classname] = constructor;

        /// <summary>
        /// Read a pickled object representation from the given memory buffer
        /// </summary>
        /// <returns>reconstituted object hierarchy specified in the buffer</returns>
        public object load(ReadOnlySpan<byte> buffer)
        {
            stack = new UnpickleStack();

            var slice = buffer;
            while (!slice.IsEmpty)
            {
                byte key = slice[0];
                int bytesConsumed = consume(key, slice.Slice(1));
                slice = slice.Slice(1 + bytesConsumed);
            }

            // TODO adsitnik: remember the size

            object value = stack.pop();
            stack.clear();
            return value; 
        }

        protected virtual object persistentLoad(string pid) 
            => throw new PickleException("A load persistent id instruction was encountered, but no persistentLoad function was specified. (implement it in custom Unpickler subclass)");

        /// <summary>
        /// Process a single pickle stream opcode
        /// </summary>
        /// <returns>number of consumed bytes</returns>
        private int consume(byte key, in ReadOnlySpan<byte> buffer)
        {
            switch (key)
            {
                case Opcodes.BINFLOAT:
                    return load_binfloat(buffer);
                case Opcodes.MARK:
                    load_mark();
                    return 0;
                case Opcodes.STOP:
                    return 0;
                case Opcodes.POP:
                    load_pop();
                    return 0;
                case Opcodes.POP_MARK:
                    load_pop_mark();
                    return 0;
                case Opcodes.DUP:
                    load_dup();
                    return 0;
                case Opcodes.FLOAT:
                    return load_float(buffer);
                case Opcodes.INT:
                    return load_int(buffer);
                case Opcodes.BININT:
                    return load_binint(buffer);
                case Opcodes.BININT1:
                    return load_binint1(buffer);
                case Opcodes.LONG:
                    return load_long(buffer);
                case Opcodes.BININT2:
                    return load_binint2(buffer);
                case Opcodes.NONE:
                    load_none();
                    return 0;
                case Opcodes.PERSID:
                    return load_persid(buffer);
                case Opcodes.BINPERSID:
                    load_binpersid();
                    return 0;
                case Opcodes.REDUCE:
                    load_reduce();
                    return 0;
                case Opcodes.STRING:
                    return load_string(buffer);
                case Opcodes.BINSTRING:
                    return load_binstring(buffer);
                case Opcodes.SHORT_BINSTRING:
                    return load_short_binstring(buffer);
                case Opcodes.UNICODE:
                    return load_unicode(buffer);
                case Opcodes.BINUNICODE:
                    return load_binunicode(buffer);
                case Opcodes.APPEND:
                    load_append();
                    return 0;
                case Opcodes.BUILD:
                    load_build();
                    return 0;
                case Opcodes.GLOBAL:
                    return load_global(buffer);
                case Opcodes.DICT:
                    load_dict();
                    return 0;
                case Opcodes.EMPTY_DICT:
                    load_empty_dictionary();
                    return 0;
                case Opcodes.APPENDS:
                    load_appends();
                    return 0;
                case Opcodes.GET:
                    return load_get(buffer);
                case Opcodes.BINGET:
                    return load_binget(buffer);
                case Opcodes.INST:
                    return load_inst(buffer);
                case Opcodes.LONG_BINGET:
                    return load_long_binget(buffer);
                case Opcodes.LIST:
                    load_list();
                    return 0;
                case Opcodes.EMPTY_LIST:
                    load_empty_list();
                    return 0;
                case Opcodes.OBJ:
                    load_obj();
                    return 0;
                case Opcodes.PUT:
                    return load_put(buffer);
                case Opcodes.BINPUT:
                    return load_binput(buffer);
                case Opcodes.LONG_BINPUT:
                    return load_long_binput(buffer);
                case Opcodes.SETITEM:
                    load_setitem();
                    return 0;
                case Opcodes.TUPLE:
                    load_tuple();
                    return 0;
                case Opcodes.EMPTY_TUPLE:
                    load_empty_tuple();
                    return 0;
                case Opcodes.SETITEMS:
                    load_setitems();
                    return 0;

                // protocol 2
                case Opcodes.PROTO:
                    return load_proto(buffer);
                case Opcodes.NEWOBJ:
                    load_newobj();
                    return 0;
                case Opcodes.EXT1:
                case Opcodes.EXT2:
                case Opcodes.EXT4:
                    throw new PickleException("Unimplemented opcode EXT1/EXT2/EXT4 encountered. Don't use extension codes when pickling via copyreg.add_extension() to avoid this error.");
                case Opcodes.TUPLE1:
                    load_tuple1();
                    return 0;
                case Opcodes.TUPLE2:
                    load_tuple2();
                    return 0;
                case Opcodes.TUPLE3:
                    load_tuple3();
                    return 0;
                case Opcodes.NEWTRUE:
                    load_true();
                    return 0;
                case Opcodes.NEWFALSE:
                    load_false();
                    return 0;
                case Opcodes.LONG1:
                    return load_long1(buffer);
                case Opcodes.LONG4:
                    return load_long4(buffer);

                // Protocol 3 (Python 3.x)
                case Opcodes.BINBYTES:
                    return load_binbytes(buffer);
                case Opcodes.SHORT_BINBYTES:
                    return load_short_binbytes(buffer);

                // Protocol 4 (Python 3.4+)
                case Opcodes.BINUNICODE8:
                    return load_binunicode8(buffer);
                case Opcodes.SHORT_BINUNICODE:
                    return load_short_binunicode(buffer);
                case Opcodes.BINBYTES8:
                    return load_binbytes8(buffer);
                case Opcodes.EMPTY_SET:
                    load_empty_set();
                    return 0;
                case Opcodes.ADDITEMS:
                    load_additems();
                    return 0;
                case Opcodes.FROZENSET:
                    load_frozenset();
                    return 0;
                case Opcodes.MEMOIZE:
                    load_memoize();
                    return 0;
                case Opcodes.FRAME:
                    return load_frame();
                case Opcodes.NEWOBJ_EX:
                    load_newobj_ex();
                    return 0;
                case Opcodes.STACK_GLOBAL:
                    load_stack_global();
                    return 0;

                default:
                    throw new InvalidOpcodeException("invalid pickle opcode: " + key);
            }
        }

        private void load_build()
        {
            object args = stack.pop();
            object target = stack.peek();
            object[] arguments = { args };
            Type[] argumentTypes = { args.GetType() };

            // call the __setstate__ method with the given arguments
            try
            {
                MethodInfo setStateMethod = target.GetType().GetMethod("__setstate__", argumentTypes);
                if (setStateMethod == null)
                {
                    throw new PickleException($"no __setstate__() found in type {target.GetType()} with argument type {args.GetType()}");
                }
                setStateMethod.Invoke(target, arguments);
            }
            catch (Exception e)
            {
                throw new PickleException("failed to __setstate__()", e);
            }
        }

        private int load_proto(in ReadOnlySpan<byte> buffer)
        {
            byte proto = buffer[0];
            if (proto > HIGHEST_PROTOCOL)
                throw new PickleException("unsupported pickle protocol: " + proto);
            return sizeof(byte);
        }

        private void load_none() => stack.add(null);

        private void load_false() => stack.add(boxedFalse);

        private void load_true() => stack.add(boxedTrue);

        private int load_int(in ReadOnlySpan<byte> memory)
        {
            int lineEndCharIndex = GetLineEndIndex(memory);

            if (lineEndCharIndex == 2 && memory[0] == (byte)'0')
            {
                if (memory[1] == (byte)'0')
                {
                    load_false();
                    return lineEndCharIndex + 1;
                }
                else if (memory[1] == (byte)'1')
                {
                    load_true();
                    return lineEndCharIndex + 1;
                }
            }

            ReadOnlySpan<byte> slice = memory.Slice(0, lineEndCharIndex);
            if (Utf8Parser.TryParse(slice, out int intNumber, out int bytesConsumed) && bytesConsumed == lineEndCharIndex)
            {
                stack.add(intNumber);
            }
            else if (Utf8Parser.TryParse(slice, out long longNumber, out bytesConsumed) && bytesConsumed == lineEndCharIndex)
            {
                stack.add(longNumber);
            }
            else
            {
                stack.add(long.Parse(PickleUtils.rawStringFromBytes(slice)));
                Debug.Fail("long.Parse should have thrown.");
            }

            return lineEndCharIndex + 1;
        }

        private int load_binint(in ReadOnlySpan<byte> memory)
        {
            int integer = BinaryPrimitives.ReadInt32LittleEndian(memory.Slice(0, sizeof(int)));
            stack.add(integer);
            return sizeof(int);
        }

        private int load_binint1(in ReadOnlySpan<byte> memory)
        {
            stack.add(memory[0]);
            return sizeof(byte);
        }

        private int load_binint2(in ReadOnlySpan<byte> memory)
        {
            int integer = BinaryPrimitives.ReadUInt16LittleEndian(memory.Slice(0, sizeof(short)));
            stack.add(integer);
            return sizeof(short);
        }

        private int load_long(in ReadOnlySpan<byte> memory)
        {
            int lineEndCharIndex = GetLineEndIndex(memory);

            ReadOnlySpan<byte> slice =
                memory[lineEndCharIndex - 1] == (byte)'L'
                    ? memory.Slice(0, lineEndCharIndex - 1)
                    : memory.Slice(0, lineEndCharIndex);

            if (!Utf8Parser.TryParse(slice, out long longvalue, out _))
            {
                stack.add(longvalue);
                return lineEndCharIndex + 1;
            }

            throw new PickleException("long too large in load_long (need BigInt)");
        }

        private int load_long1(in ReadOnlySpan<byte> memory)
        {
            byte length = memory[0];
            stack.add(BinaryPrimitives.ReadInt64LittleEndian(memory.Slice(sizeof(byte), length)));
            return sizeof(byte) + length;
        }

        private int load_long4(in ReadOnlySpan<byte> memory)
        {
            int length = BinaryPrimitives.ReadInt32LittleEndian(memory.Slice(0, sizeof(int)));
            stack.add(BinaryPrimitives.ReadInt64LittleEndian(memory.Slice(sizeof(int), length)));
            return sizeof(int) + length;
        }

        private int load_float(in ReadOnlySpan<byte> memory)
        {
            int lineEndCharIndex = GetLineEndIndex(memory);

            if (Utf8Parser.TryParse(memory.Slice(0, lineEndCharIndex), out double d, out _))
            {
                stack.add(d);
                return lineEndCharIndex + 1;
            }

            throw new FormatException();
        }

        private int load_binfloat(in ReadOnlySpan<byte> buffer)
        {
            double val = BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64BigEndian(buffer.Slice(0, sizeof(long))));
            stack.add(val);
            return sizeof(long);
        }

        private int load_string(in ReadOnlySpan<byte> buffer)
        {
            int lineEndCharIndex = GetLineEndIndex(buffer);
            string rep = PickleUtils.rawStringFromBytes(buffer.Slice(0, lineEndCharIndex));

            bool quotesOk = false;
            foreach (string q in quoteStrings) // double or single quote
            {
                if (rep.StartsWith(q))
                {
                    if (!rep.EndsWith(q))
                    {
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

            return lineEndCharIndex + 1;
        }

        private int load_binstring(in ReadOnlySpan<byte> buffer)
        {
            int length = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, sizeof(int)));
            stack.add(PickleUtils.rawStringFromBytes(buffer.Slice(sizeof(int), length)));
            return sizeof(int) + length;
        }

        private int load_binbytes(in ReadOnlySpan<byte> buffer)
        {
            int length = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, sizeof(int)));
            stack.add(buffer.Slice(sizeof(int), length).ToArray());
            return sizeof(int) + length;
        }

        private int load_binbytes8(in ReadOnlySpan<byte> buffer)
        {
            // .NET does not support arrays longer than int.MaxSize so we cast to the long to int
            int length = (int)BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(0, sizeof(long)));
            stack.add(buffer.Slice(sizeof(long), length).ToArray());
            return sizeof(long) + length;
        }

        private int load_unicode(in ReadOnlySpan<byte> buffer)
        {
            int endLineCharIndex = GetLineEndIndex(buffer);
            string str = PickleUtils.decode_unicode_escaped(PickleUtils.rawStringFromBytes(buffer.Slice(0, endLineCharIndex)));
            stack.add(str);
            return endLineCharIndex + 1;
        }

        private int load_binunicode(in ReadOnlySpan<byte> buffer)
        {
            int length = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, sizeof(int)));
            stack.add(GetStringFromUtf8(buffer.Slice(sizeof(int), length)));
            return sizeof(int) + length;
        }

        private int load_binunicode8(in ReadOnlySpan<byte> buffer)
        {
            // .NET does not support arrays longer than int.MaxSize so we cast to the long to int
            int length = (int)BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(0, sizeof(long)));
            stack.add(GetStringFromUtf8(buffer.Slice(sizeof(long), length)));
            return sizeof(long) + length;
        }

        private int load_short_binunicode(in ReadOnlySpan<byte> buffer)
        {
            byte length = buffer[0];
            stack.add(GetStringFromUtf8(buffer.Slice(sizeof(byte), length)));
            return sizeof(byte) + length;
        }

        private int load_short_binstring(in ReadOnlySpan<byte> buffer)
        {
            byte length = buffer[0];
            stack.add(PickleUtils.rawStringFromBytes(buffer.Slice(sizeof(byte), length)));
            return sizeof(byte) + length;
        }

        private int load_short_binbytes(in ReadOnlySpan<byte> buffer)
        {
            byte length = buffer[0];
            stack.add(buffer.Slice(sizeof(byte), length).ToArray());
            return sizeof(byte) + length;
        }

        private void load_tuple() => stack.add(stack.pop_all_since_marker_as_array());

        private void load_empty_tuple() => stack.add(Array.Empty<object>());

        private void load_tuple1() => stack.add(new[] { stack.pop() });

        private void load_tuple2()
        {
            object o2 = stack.pop();
            object o1 = stack.pop();
            stack.add(new[] { o1, o2 });
        }

        private void load_tuple3()
        {
            object o3 = stack.pop();
            object o2 = stack.pop();
            object o1 = stack.pop();
            stack.add(new[] { o1, o2, o3 });
        }

        private void load_empty_list() => stack.add(new ArrayList(5));

        private void load_empty_dictionary() => stack.add(new Hashtable(5));

        private void load_empty_set() => stack.add(new HashSet<object>());

        private void load_list() => stack.add(stack.pop_all_since_marker()); // simply add the top items as a list to the stack again

        private void load_dict()
        {
            object[] top = stack.pop_all_since_marker_as_array();
            Hashtable map = new Hashtable(top.Length);
            for (int i = 0; i < top.Length; i += 2)
            {
                object key = top[i];
                object value = top[i + 1];
                map[key] = value;
            }
            stack.add(map);
        }

        private void load_frozenset()
        {
            object[] top = stack.pop_all_since_marker_as_array();
            var set = new HashSet<object>();
            foreach (var element in top)
                set.Add(element);
            stack.add(set);
        }

        private void load_additems()
        {
            object[] top = stack.pop_all_since_marker_as_array();
            var set = (HashSet<object>)stack.pop();
            foreach (object item in top)
                set.Add(item);
            stack.add(set);
        }

        private int load_global(in ReadOnlySpan<byte> buffer)
        {
            int moduleLineEndCharIndex = GetLineEndIndex(buffer);
            string module = GetStringFromUtf8(buffer.Slice(0, moduleLineEndCharIndex));

            int nameLineEndCharIndex = GetLineEndIndex(buffer.Slice(moduleLineEndCharIndex + 1));
            string name = GetStringFromUtf8(buffer.Slice(moduleLineEndCharIndex + 1, nameLineEndCharIndex));

            load_global_sub(module, name);

            return moduleLineEndCharIndex + 1 + nameLineEndCharIndex + 1;
        }

        private void load_stack_global()
        {
            string name = (string)stack.pop();
            string module = (string)stack.pop();
            load_global_sub(module, name);
        }

        private string GetModuleNameKey(string module, string name)
        {
            if (concatenatedModuleNames == null)
            {
                concatenatedModuleNames = new Dictionary<StringPair, string>();
            }

            var sp = new StringPair(module, name);
            if (!concatenatedModuleNames.TryGetValue(sp, out string key))
            {
                key = module + "." + name;
                concatenatedModuleNames.Add(sp, key);
            }

            return key;
        }

        private void load_global_sub(string module, string name)
        {
            if (!objectConstructors.TryGetValue(GetModuleNameKey(module, name), out IObjectConstructor constructor))
            {
                switch (module)
                {
                    // check if it is an exception
                    case "exceptions":
                        // python 2.x
                        constructor = new ExceptionConstructor(typeof(PythonException), module, name);
                        break;
                    case "builtins":
                    case "__builtin__":
                        if (name.EndsWith("Error") || name.EndsWith("Warning") || name.EndsWith("Exception")
                            || name == "GeneratorExit" || name == "KeyboardInterrupt"
                            || name == "StopIteration" || name == "SystemExit")
                        {
                            // it's a python 3.x exception
                            constructor = new ExceptionConstructor(typeof(PythonException), module, name);
                        }
                        else
                        {
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

        private void load_pop() => stack.pop();

        private void load_pop_mark()
        {
            object o;
            do
            {
                o = stack.pop();
            } while (o != stack.MARKER);
            stack.trim();
        }

        private void load_dup() => stack.add(stack.peek());

        private int load_get(in ReadOnlySpan<byte> buffer)
        {
            int lineEndCharIndex = GetLineEndIndex(buffer);
            if (!Utf8Parser.TryParse(buffer.Slice(0, lineEndCharIndex), out int key, out _))
                key = int.Parse(PickleUtils.rawStringFromBytes(buffer.Slice(0, lineEndCharIndex)));
            if (!memo.TryGetValue(key, out var value))
                throw new PickleException("invalid memo key");
            stack.add(value);
            return lineEndCharIndex + 1;
        }

        private int load_binget(in ReadOnlySpan<byte> buffer)
        {
            byte key = buffer[0];
            if (!memo.TryGetValue(key, out var value))
                throw new PickleException("invalid memo key");
            stack.add(value);
            return sizeof(byte);
        }

        private int load_long_binget(in ReadOnlySpan<byte> buffer)
        {
            int key = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, sizeof(int)));
            if (!memo.TryGetValue(key, out var value))
                throw new PickleException("invalid memo key");
            stack.add(value);
            return sizeof(int);
        }

        private int load_put(in ReadOnlySpan<byte> buffer)
        {
            int lineEndCharIndex = GetLineEndIndex(buffer);
            if (!Utf8Parser.TryParse(buffer.Slice(0, lineEndCharIndex), out int index, out _))
                index = int.Parse(PickleUtils.rawStringFromBytes(buffer.Slice(0, lineEndCharIndex)));

            memo[index] = stack.peek();

            return lineEndCharIndex + 1;
        }

        private int load_binput(in ReadOnlySpan<byte> buffer)
        {
            byte index = buffer[0];
            memo[index] = stack.peek();
            return sizeof(byte);
        }

        private void load_memoize() => memo[memo.Count] = stack.peek();

        private int load_long_binput(in ReadOnlySpan<byte> buffer)
        {
            int index = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(0, sizeof(int)));
            memo[index] = stack.peek();
            return sizeof(int);
        }

        private void load_append()
        {
            object value = stack.pop();
            ArrayList list = (ArrayList)stack.peek();
            list.Add(value);
        }

        private void load_appends()
        {
            object[] top = stack.pop_all_since_marker_as_array();
            ArrayList list = (ArrayList)stack.peek();
            for (int i = 0; i < top.Length; i++)
            {
                list.Add(top[i]);
            }
        }

        private void load_setitem()
        {
            object value = stack.pop();
            object key = stack.pop();
            Hashtable dict = (Hashtable)stack.peek();
            dict[key] = value;
        }

        private void load_setitems()
        {
            var newitems = new List<KeyValuePair<object, object>>();
            object value = stack.pop();
            while (value != stack.MARKER)
            {
                object key = stack.pop();
                newitems.Add(new KeyValuePair<object, object>(key, value));
                value = stack.pop();
            }

            Hashtable dict = (Hashtable)stack.peek();
            foreach (var item in newitems)
            {
                dict[item.Key] = item.Value;
            }
        }

        private void load_mark() => stack.add_mark();

        private void load_reduce()
        {
            var args = (object[])stack.pop();
            IObjectConstructor constructor = (IObjectConstructor)stack.pop();
            stack.add(constructor.construct(args));
        }

        private void load_newobj() => load_reduce(); // we just do the same as class(*args) instead of class.__new__(class,*args)

        private void load_newobj_ex()
        {
            Hashtable kwargs = (Hashtable)stack.pop();
            var args = (object[])stack.pop();
            IObjectConstructor constructor = (IObjectConstructor)stack.pop();
            if (kwargs.Count == 0)
                stack.add(constructor.construct(args));
            else
                throw new PickleException("newobj_ex with keyword arguments not supported");
        }

        private int load_frame() => sizeof(long); // for now we simply skip the frame opcode and its length

        private int load_persid(in ReadOnlySpan<byte> buffer)
        {
            // the persistent id is taken from the argument
            int lineEndCharIndex = GetLineEndIndex(buffer);
            string pid = PickleUtils.rawStringFromBytes(buffer.Slice(0, lineEndCharIndex));
            stack.add(persistentLoad(pid));
            return lineEndCharIndex + 1;
        }

        private void load_binpersid()
        {
            // the persistent id is taken from the stack
            string pid = stack.pop().ToString();
            stack.add(persistentLoad(pid));
        }

        private void load_obj()
        {
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

        private int load_inst(in ReadOnlySpan<byte> buffer)
        {
            int moduleLineEndCharIndex = GetLineEndIndex(buffer);
            string module = GetStringFromUtf8(buffer.Slice(0, moduleLineEndCharIndex));

            int nameLineEndCharIndex = GetLineEndIndex(buffer.Slice(moduleLineEndCharIndex + 1));
            string className = GetStringFromUtf8(buffer.Slice(moduleLineEndCharIndex + 1, nameLineEndCharIndex));

            object[] args = stack.pop_all_since_marker_as_array();

            if (!objectConstructors.TryGetValue(GetModuleNameKey(module, className), out IObjectConstructor constructor))
            {
                constructor = new ClassDictConstructor(module, className);
                args = Array.Empty<object>(); // classdict doesn't have constructor args... so we may lose info here, hmm.
            }
            stack.add(constructor.construct(args));

            return moduleLineEndCharIndex + 1 + nameLineEndCharIndex + 1;
        }

        private int GetLineEndIndex(in ReadOnlySpan<byte> memory) => memory.IndexOf((byte)'\n');

        private string GetStringFromUtf8(in ReadOnlySpan<byte> utf8)
        {
            unsafe
            {
                fixed (byte* bytes = utf8)
                {
                    return Encoding.UTF8.GetString(bytes, utf8.Length);
                }
            }
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

        private readonly struct StringPair : IEquatable<StringPair>
        {
            public readonly string Item1, Item2;
            public StringPair(string item1, string item2) { Item1 = item1; Item2 = item2; }
            public bool Equals(StringPair other) => Item1 == other.Item1 && Item2 == other.Item2;
            public override bool Equals(object obj) => obj is StringPair sp && Equals(sp);
            public override int GetHashCode() => Item1.GetHashCode() ^ Item2.GetHashCode();
        }
    }
}
