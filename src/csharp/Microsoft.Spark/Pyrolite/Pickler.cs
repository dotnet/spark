// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Buffers.Binary;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable MemberInitializerValueIgnored
// ReSharper disable InconsistentNaming
// ReSharper disable LoopCanBeConvertedToQuery
// ReSharper disable InvertIf
// ReSharper disable SuggestBaseTypeForParameter

namespace Razorvine.Pickle
{
	
/// <summary>
/// Pickle an object graph into a Python-compatible pickle stream. For
/// simplicity, the only supported pickle protocol at this time is protocol 2. 
/// See README.txt for a table with the type mapping.
/// This class is NOT threadsafe! (Don't use the same pickler from different threads)
/// </summary>
internal class Pickler : IDisposable {

	// ReSharper disable once UnusedMember.Global
	public static int HIGHEST_PROTOCOL = 2;
	protected const int MAX_RECURSE_DEPTH = 200;
	protected const int PROTOCOL = 2;

	protected static readonly IDictionary<Type, IObjectPickler> customPicklers = new Dictionary<Type, IObjectPickler>();
    private static readonly byte[] datetimeDatetimeBytes = Encoding.ASCII.GetBytes("datetime\ndatetime\n");
    private static readonly byte[] datetimeTimedeltaBytes = Encoding.ASCII.GetBytes("datetime\ntimedelta\n");
    private static readonly byte[] builtinSetBytes = Encoding.ASCII.GetBytes("__builtin__\nset\n");
    private static readonly byte[] builtinBytearrayBytes = Encoding.ASCII.GetBytes("__builtin__\nbytearray\n");
    private static readonly byte[] arrayArrayBytes = Encoding.ASCII.GetBytes("array\narray\n");
    private static readonly byte[] decimalDecimalBytes = Encoding.ASCII.GetBytes("decimal\nDecimal\n");

    protected readonly bool useMemo=true;
    private readonly byte[] byteBuffer = new byte[sizeof(long)]; // at least large enough for any primitive being serialized
	protected Stream outs;
	protected int recurse;	// recursion level
	protected IDictionary<object, int> memo;		// maps objects to memo index
	
	/**
	 * Create a Pickler.
	 */
	public Pickler() : this(true) {
	}

	/**
	 * Create a Pickler. Specify if it is to use a memo table or not.
	 * The memo table is NOT reused across different calls.
	 * If you use a memo table, you can only pickle objects that are hashable.
	 */
	public Pickler(bool useMemo) {
		this.useMemo=useMemo;
	}
	
	/**
	 * Close the pickler stream, discard any internal buffers.
	 */
	public void close() {
		memo = null;
		outs.Flush();
		outs.Close();
	}

	/**
	 * Register additional object picklers for custom classes.
	 * If you register an interface or abstract base class, it means the pickler is used for 
	 * the whole inheritance tree of all classes ultimately implementing that interface or abstract base class.
	 * If you register a normal concrete class, the pickler is only used for objects of exactly that particular class.
	 */
	public static void registerCustomPickler(Type clazz, IObjectPickler pickler) {
		customPicklers[clazz]=pickler;
	}
	
	/**
	 * Pickle a given object graph, returning the result as a byte array.
	 */
	public byte[] dumps(object o) {
		MemoryStream bo = new MemoryStream();
		dump(o, bo);
		bo.Flush();
		return bo.ToArray();
	}

	/**
	 * Pickle a given object graph, writing the result to the output stream.
	 */
	public void dump(object o, Stream stream) {
		outs = stream;
		recurse = 0;
		if(useMemo)
			memo = new Dictionary<object, int>();
		outs.WriteByte(Opcodes.PROTO);
		outs.WriteByte(PROTOCOL);
		save(o);
		memo = null;  // get rid of the memo table
		outs.WriteByte(Opcodes.STOP);
		outs.Flush();
		if(recurse!=0)  // sanity check
			throw new PickleException("recursive structure error, please report this problem");
	}

	/**
	 * Pickle a single object and write its pickle representation to the output stream.
	 * Normally this is used internally by the pickler, but you can also utilize it from
	 * within custom picklers. This is handy if as part of the custom pickler, you need
	 * to write a couple of normal objects such as strings or ints, that are already
	 * supported by the pickler.
	 * This method can be called recursively to output sub-objects.
	 */
	public void save(object o) {
		recurse++;
		if(recurse>MAX_RECURSE_DEPTH)
			throw new StackOverflowException("recursion too deep in Pickler.save (>"+MAX_RECURSE_DEPTH+")");

		// null type?
		if(o==null) {
			outs.WriteByte(Opcodes.NONE);
			recurse--;
			return;
		}


		Type t=o.GetType();
		
		// check the memo table, otherwise simply dispatch
		if((useMemo && LookupMemo(t, o)) || dispatch(t, o)){
			recurse--;
			return;
		}

		throw new PickleException("couldn't pickle object of type "+t);
	}
	
	/**
	 * Write the object to the memo table and output a memo write opcode
	 * Only works for hashable objects
	 */
	protected void WriteMemo<T>(T value) {
        if (useMemo)
            WriteMemoPrivate(value);

        void WriteMemoPrivate(object obj)
        {
		    if(!memo.ContainsKey(obj))
		    {
			    int memo_index = memo.Count;
			    memo[obj] = memo_index;
			    if(memo_index<=0xFF)
			    {
				    outs.WriteByte(Opcodes.BINPUT);
				    outs.WriteByte((byte)memo_index);
			    }
			    else
			    {
				    outs.WriteByte(Opcodes.LONG_BINPUT);
                    BinaryPrimitives.WriteInt32LittleEndian(byteBuffer, memo_index);
				    outs.Write(byteBuffer, 0, sizeof(int));
			    }
		    }
	    }
    }
	
	/**
	 * Check the memo table and output a memo lookup if the object is found
	 */
	private bool LookupMemo(Type objectType, object obj)
    {
        Debug.Assert(useMemo);

		if(!objectType.IsPrimitive)
		{
			int memo_index;
			if(memo.TryGetValue(obj, out memo_index))
			{
				if(memo_index<=0xff)
				{
					outs.WriteByte(Opcodes.BINGET);
					outs.WriteByte((byte)memo_index);
				}
				else
				{
					outs.WriteByte(Opcodes.LONG_BINGET);
                    BinaryPrimitives.WriteInt32LittleEndian(byteBuffer, memo_index);
					outs.Write(byteBuffer, 0, sizeof(int));
				}
				return true;
			}
		}
		return false;
	}

	/**
	 * Process a single object to be pickled.
	 */
	private bool dispatch(Type t, object o) {
        Debug.Assert(t != null);
        Debug.Assert(o != null);
        Debug.Assert(t == o.GetType());

        // is it a primitive array?
        if (o is Array) {
			Type componentType=t.GetElementType();
			if(componentType != null && componentType.IsPrimitive) {
				put_arrayOfPrimitives(componentType, o);
			} else {
				put_arrayOfObjects((object[])o);
			}
			return true;
		}

        // first check for enums, as GetTypeCode will return the underlying type.
		if(o is Enum) {
			put_string(o.ToString());
			return true;
		}
		
		// first the primitive types
        switch (Type.GetTypeCode(t))
        {
            case TypeCode.Boolean:
			    put_bool((bool)o);
			    return true;
		    case TypeCode.Byte:
			    put_long((byte)o);
			    return true;
            case TypeCode.SByte:
			    put_long((sbyte)o);
			    return true;
            case TypeCode.Int16:
			    put_long((short)o);
			    return true;
            case TypeCode.UInt16:
			    put_long((ushort)o);
			    return true;
            case TypeCode.Int32:
                put_long((int)o);
                return true;
            case TypeCode.UInt32:
                put_long((uint)o);
                return true;
            case TypeCode.Int64:
                put_long((long)o);
			    return true;
            case TypeCode.UInt64:
                put_ulong((ulong)o);
                return true;
            case TypeCode.Single:
			    put_float((float)o);
			    return true;
            case TypeCode.Double:
			    put_float((double)o);
			    return true;
            case TypeCode.Char:
			    put_string(((char)o).ToString());
			    return true;
            case TypeCode.String:
                put_string((string)o);
                return true;
            case TypeCode.Decimal:
                put_decimal((decimal)o);
                return true;
            case TypeCode.DateTime:
                put_datetime((DateTime)o);
                return true;
        }
		
		// check registry
		IObjectPickler custompickler = getCustomPickler(t);
		if(custompickler!=null) {
			custompickler.pickle(o, outs, this);
			WriteMemo(o);
			return true;
		}
		
		// more complex types
		if(o is TimeSpan) {
			put_timespan((TimeSpan)o);
			return true;
		}
		if(t.IsGenericType && t.GetGenericTypeDefinition()==typeof(HashSet<>)) {
			put_set((IEnumerable)o);
			return true;
		}

		var dictionary = o as IDictionary;
		if(dictionary != null) {
			put_map(dictionary);
			return true;
		}

		var list = o as IList;
		if(list != null) {
			put_enumerable(list);
			return true;
		}

		var enumerable = o as IEnumerable;
		if(enumerable != null) {
			put_enumerable(enumerable);
			return true;
		}
		
		DataContractAttribute dca = (DataContractAttribute) Attribute.GetCustomAttribute(t, typeof(DataContractAttribute));
		if(dca!=null) {
			put_datacontract(t, o, dca);
			return true;
		}
		
		SerializableAttribute sa = (SerializableAttribute) Attribute.GetCustomAttribute(t, typeof(SerializableAttribute));
		if(sa!=null) {
			put_serializable(t, o);
			return true;
		}
		
		if(hasPublicProperties(o)) {
			put_objwithproperties(o);
			return true;
		}

		return false;
	}
	
	[SuppressMessage("ReSharper", "MemberCanBeMadeStatic.Global")]
	protected IObjectPickler getCustomPickler(Type t)
	{
		IObjectPickler pickler;
		if(customPicklers.TryGetValue(t, out pickler))
			return pickler;		// exact match
		
		// check if there's a custom pickler registered for an interface or abstract base class
		// that this object implements or inherits from.
		foreach(var x in customPicklers) {
			if(x.Key.IsAssignableFrom(t)) {
				return x.Value;
			}
		}

		return null;
	}

	private static bool hasPublicProperties(object o)
	{
		var props=o.GetType().GetProperties();
		return props.Length>0;
	}

	private void put_datetime(DateTime dt) {
		outs.WriteByte(Opcodes.GLOBAL);
		outs.Write(datetimeDatetimeBytes, 0, datetimeDatetimeBytes.Length);
		outs.WriteByte(Opcodes.MARK);
        put_long(dt.Year);
        put_long(dt.Month);
        put_long(dt.Day);
        put_long(dt.Hour);
        put_long(dt.Minute);
        put_long(dt.Second);
        put_long(dt.Millisecond * 1000);
		outs.WriteByte(Opcodes.TUPLE);
		outs.WriteByte(Opcodes.REDUCE);
        WriteMemo(dt);
	}

	private void put_timespan(TimeSpan ts) {
		outs.WriteByte(Opcodes.GLOBAL);
		outs.Write(datetimeTimedeltaBytes, 0, datetimeTimedeltaBytes.Length);
        put_long(ts.Days);
		put_long(ts.Hours*3600+ts.Minutes*60+ts.Seconds);
        put_long(ts.Milliseconds*1000);
		outs.WriteByte(Opcodes.TUPLE3);
		outs.WriteByte(Opcodes.REDUCE);
        WriteMemo(ts);
	}

	private void put_enumerable(IEnumerable list) {
		outs.WriteByte(Opcodes.EMPTY_LIST);
		WriteMemo(list);
		outs.WriteByte(Opcodes.MARK);
		foreach(var o in list) {
			save(o);
		}
		outs.WriteByte(Opcodes.APPENDS);
	}

	private void put_map(IDictionary o) {
		outs.WriteByte(Opcodes.EMPTY_DICT);
		WriteMemo(o);
		outs.WriteByte(Opcodes.MARK);
		foreach(var k in o.Keys) {
			save(k);
			save(o[k]);
		}
		outs.WriteByte(Opcodes.SETITEMS);
	}

	private void put_set(IEnumerable o) {
		outs.WriteByte(Opcodes.GLOBAL);
		outs.Write(builtinSetBytes, 0, builtinSetBytes.Length);
		outs.WriteByte(Opcodes.EMPTY_LIST);
		outs.WriteByte(Opcodes.MARK);
		foreach(object x in o) {
			save(x);
		}
		outs.WriteByte(Opcodes.APPENDS);
		outs.WriteByte(Opcodes.TUPLE1);
		outs.WriteByte(Opcodes.REDUCE);
		WriteMemo(o);   // sets cannot contain self-references (because not hashable) so it is fine to put this at the end
	}

	private void put_arrayOfObjects(object[] array)
	{
		switch (array.Length)
		{
			// 0 objects->EMPTYTUPLE
			// 1 object->TUPLE1
			// 2 objects->TUPLE2
			// 3 objects->TUPLE3
			// 4 or more->MARK+items+TUPLE
			case 0:
				outs.WriteByte(Opcodes.EMPTY_TUPLE);
				break;
			case 1:
				if(array[0]==array)
				    ThrowRecursiveArrayNotSupported();
				save(array[0]);
				outs.WriteByte(Opcodes.TUPLE1);
				break;
			case 2:
				if(array[0]==array || array[1]==array)
				    ThrowRecursiveArrayNotSupported();
				save(array[0]);
				save(array[1]);
				outs.WriteByte(Opcodes.TUPLE2);
				break;
			case 3:
				if(array[0]==array || array[1]==array || array[2]==array)
				    ThrowRecursiveArrayNotSupported();
				save(array[0]);
				save(array[1]);
				save(array[2]);
				outs.WriteByte(Opcodes.TUPLE3);
				break;
			default:
				outs.WriteByte(Opcodes.MARK);
				foreach(object o in array) {
					if(o==array)
						ThrowRecursiveArrayNotSupported();
					save(o);
				}
				outs.WriteByte(Opcodes.TUPLE);
				break;
		}

		WriteMemo(array);		// tuples cannot contain self-references so it is fine to put this at the end
	}

    private static void ThrowRecursiveArrayNotSupported() =>
        throw new PickleException("recursive array not supported, use list");

    private void put_arrayOfPrimitives(Type t, object array) {
		TypeCode typeCode = Type.GetTypeCode(t);

        // Special-case several array types written out specially.
        switch (typeCode)
        {
            case TypeCode.Boolean:
			    // a bool[] isn't written as an array but rather as a tuple
			    var source=(bool[])array;
			    // this is stupid, but seems to be necessary because you can't cast a bool[] to an object[]
			    var boolarray=new object[source.Length];
			    Array.Copy(source, boolarray, source.Length);
			    put_arrayOfObjects(boolarray);
			    return;

            case TypeCode.Char:
			    // a char[] isn't written as an array but rather as a unicode string
			    string s=new string((char[])array);
			    put_string(s);
			    return;

            case TypeCode.Byte:
			    // a byte[] isn't written as an array but rather as a bytearray object
			    outs.WriteByte(Opcodes.GLOBAL);
			    outs.Write(builtinBytearrayBytes, 0, builtinBytearrayBytes.Length);
			    put_string(PickleUtils.rawStringFromBytes((byte[])array));
			    put_string("latin-1");	// this is what python writes in the pickle
			    outs.WriteByte(Opcodes.TUPLE2);
			    outs.WriteByte(Opcodes.REDUCE);
			    WriteMemo(array);
			    return;
		} 
		
		outs.WriteByte(Opcodes.GLOBAL);
		outs.Write(arrayArrayBytes, 0, arrayArrayBytes.Length);
		outs.WriteByte(Opcodes.SHORT_BINSTRING); // array typecode follows
		outs.WriteByte(1); // typecode is 1 char

        switch (typeCode)
        {
            case TypeCode.SByte:
			    outs.WriteByte((byte)'b'); // signed char
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(sbyte s in (sbyte[])array) {
			        put_long(s);
			    }
                break;

            case TypeCode.Int16:
			    outs.WriteByte((byte)'h'); // signed short
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(short s in (short[])array) {
			        put_long(s);
			    }
                break;

            case TypeCode.UInt16:
			    outs.WriteByte((byte)'H'); // unsigned short
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(ushort s in (ushort[])array) {
			        put_long(s);
			    }
                break;

            case TypeCode.Int32:
			    outs.WriteByte((byte)'i'); // signed int
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(int i in (int[])array) {
			        put_long(i);
			    }
                break;

            case TypeCode.UInt32:
			    outs.WriteByte((byte)'I'); // unsigned int
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(uint i in (uint[])array) {
			        put_long(i);
			    }
                break;

            case TypeCode.Int64:
			    outs.WriteByte((byte)'l');  // signed long
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(long v in (long[])array) {
			        put_long(v);
			    }
                break;

            case TypeCode.UInt64:
			    outs.WriteByte((byte)'L');  // unsigned long
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(ulong v in (ulong[])array) {
			        put_ulong(v);
			    }
                break;

            case TypeCode.Single:
			    outs.WriteByte((byte)'f');  // float
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(float f in (float[])array) {
			        put_float(f);
			    }
                break;

            case TypeCode.Double:
			    outs.WriteByte((byte)'d');  // double
			    outs.WriteByte(Opcodes.EMPTY_LIST);
			    outs.WriteByte(Opcodes.MARK);
			    foreach(double d in (double[])array) {
			        put_float(d);
			    }
                break;
		}
		
		outs.WriteByte(Opcodes.APPENDS);
		outs.WriteByte(Opcodes.TUPLE2);
		outs.WriteByte(Opcodes.REDUCE);

		WriteMemo(array); // array of primitives can by definition never be recursive, so okay to put this at the end
	}

	private void put_decimal(decimal d) {
		//"cdecimal\nDecimal\nU\n12345.6789\u0085R."
		outs.WriteByte(Opcodes.GLOBAL);
		outs.Write(decimalDecimalBytes, 0, decimalDecimalBytes.Length);
		put_string(d.ToString(CultureInfo.InvariantCulture));
		outs.WriteByte(Opcodes.TUPLE1);
		outs.WriteByte(Opcodes.REDUCE);
        WriteMemo(d);
	}

	private void put_string(string str) {
		outs.WriteByte(Opcodes.BINUNICODE);
		var encoded=Encoding.UTF8.GetBytes(str);
        BinaryPrimitives.WriteInt32LittleEndian(byteBuffer, encoded.Length);
		outs.Write(byteBuffer, 0, sizeof(int));
		outs.Write(encoded, 0, encoded.Length);
		WriteMemo(str);
	}

	private void put_float(double d) {
		outs.WriteByte(Opcodes.BINFLOAT);
        BinaryPrimitives.WriteInt64BigEndian(byteBuffer, BitConverter.DoubleToInt64Bits(d));
		outs.Write(byteBuffer, 0, sizeof(double));
	}

	private void put_long(long v) {
		// choose optimal representation
		// first check 1 and 2-byte unsigned ints:
		if(v>=0) {
			if(v<=0xff) {
				outs.WriteByte(Opcodes.BININT1);
				outs.WriteByte((byte)v);
				return;
			}
			if(v<=0xffff) {
				outs.WriteByte(Opcodes.BININT2);
				outs.WriteByte((byte)(v&0xff));
				outs.WriteByte((byte)(v>>8));
				return;
			}
		}
		
		// 4-byte signed int?
		long high_bits=v>>31;  // shift sign extends
		if(high_bits==0 || high_bits==-1) {
			// All high bits are copies of bit 2**31, so the value fits in a 4-byte signed int.
			outs.WriteByte(Opcodes.BININT);
            BinaryPrimitives.WriteInt32LittleEndian(byteBuffer, (int)v);
			outs.Write(byteBuffer, 0, sizeof(int));
			return;
		}
		
		// int too big, store it as text
		outs.WriteByte(Opcodes.INT);
		byte[] output=Encoding.ASCII.GetBytes(v.ToString(CultureInfo.InvariantCulture));
		outs.Write(output, 0, output.Length);
		outs.WriteByte((byte)'\n');
	}

	private void put_ulong(ulong u) {
		if(u<=long.MaxValue) {
			put_long((long)u);
		} else {
			// ulong too big for a signed long, store it as text instead.
			outs.WriteByte(Opcodes.INT);
			var output=Encoding.ASCII.GetBytes(u.ToString(CultureInfo.InvariantCulture));
			outs.Write(output, 0, output.Length);
			outs.WriteByte((byte)'\n');
		}
	}

	private void put_bool(bool b)
	{
		outs.WriteByte(b ? Opcodes.NEWTRUE : Opcodes.NEWFALSE);
	}

	private void put_objwithproperties(object o) {
		var properties=o.GetType().GetProperties();
		var map=new Dictionary<string, object>();
		foreach(var propinfo in properties) {
			if(propinfo.CanRead) {
				string name=propinfo.Name;
				try {
					map[name]=propinfo.GetValue(o, null);
				} catch (Exception x) {
					throw new PickleException("cannot pickle object:",x);
				}
			}
		}
		
		// if we're dealing with an anonymous type, don't output the type name.
		if(!o.GetType().Name.StartsWith("<>"))
			map["__class__"]=o.GetType().FullName;

		save(map);
	}

	private void put_serializable(Type t, object o)
	{
		var map=new Dictionary<string, object>();
		var fields = t.GetFields();
		foreach(var field in fields) {
			if(field.GetCustomAttribute(typeof(NonSerializedAttribute))==null) {
				string name=field.Name;
				try {
					map[name]=field.GetValue(o);
				} catch (Exception x) {
					throw new PickleException("cannot pickle [Serializable] object:",x);
				}
			}
		}
		var properties=t.GetProperties();
		foreach(var propinfo in properties) {
			if(propinfo.CanRead) {
				string name=propinfo.Name;
				try {
					map[name]=propinfo.GetValue(o, null);
				} catch (Exception x) {
					throw new PickleException("cannot pickle [Serializable] object:",x);
				}
			}
		}

		// if we're dealing with an anonymous type, don't output the type name.
		if(!o.GetType().Name.StartsWith("<>"))
			map["__class__"]=o.GetType().FullName;

		save(map);
	}

	private void put_datacontract(Type t, object o, DataContractAttribute dca)
	{
		var fields = t.GetFields();
		var map=new Dictionary<string, object>();
		foreach(var field in fields) {
			DataMemberAttribute dma = (DataMemberAttribute) field.GetCustomAttribute(typeof(DataMemberAttribute));
			if(dma!=null) {
				string name=dma.Name;
				try {
					map[name]=field.GetValue(o);
				} catch (Exception x) {
					throw new PickleException("cannot pickle [DataContract] object:",x);
				}
			}
		}
		var properties=t.GetProperties();
		foreach(var propinfo in properties) {
			if(propinfo.CanRead && propinfo.GetCustomAttribute(typeof(DataMemberAttribute))!=null) {
				string name=propinfo.Name;
				try {
					map[name]=propinfo.GetValue(o, null);
				} catch (Exception x) {
					throw new PickleException("cannot pickle [DataContract] object:",x);
				}
			}
		}

		if(string.IsNullOrEmpty(dca.Name)) {
			// if we're dealing with an anonymous type, don't output the type name.
			if(!o.GetType().Name.StartsWith("<>"))
				map["__class__"]=o.GetType().FullName;
		} else {
			map["__class__"] = dca.Name;
		}

		save(map);
	}
		
	public void Dispose()
	{
		close();
	}
}

}
