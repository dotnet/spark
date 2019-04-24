// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Buffers.Binary;
using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Razorvine.Pickle.Objects
{

/// <summary>
/// Creates arrays of objects. Returns a primitive type array such as int[] if 
/// the objects are ints, etc. 
/// </summary>
[SuppressMessage("ReSharper", "InconsistentNaming")]
[SuppressMessage("ReSharper", "MemberCanBePrivate.Global")]
[SuppressMessage("ReSharper", "MemberCanBeMadeStatic.Global")]
internal class ArrayConstructor : IObjectConstructor {

	public object construct(object[] args) {
		// args for array constructor: [ string typecode, ArrayList values ]
		// or: [ constructor_class, typecode, machinecode_type, byte[] ]
		if (args.Length==4) {
			ArrayConstructor constructor = (ArrayConstructor) args[0];
			char arraytype = ((string) args[1])[0];
			int machinecodeType = (int) args[2];
			var data = (byte[]) args[3];
			return constructor.construct(arraytype, machinecodeType, data);		    
		}
		if (args.Length != 2) {
			throw new PickleException("invalid pickle data for array; expected 2 args, got "+args.Length);
		}

		string typecode = (string) args[0];
		ArrayList values = args[1] as ArrayList;
		if(values==null) {
			// python 2.6 encodes the array as a string sequence rather than a list
			// unpickling this is not supported at this time
			throw new PickleException("unsupported Python 2.6 array pickle format");
		}

		switch (typecode[0]) {
		case 'c':// character 1 -> char[]
		case 'u':// Unicode character 2 -> char[]
		{
			var result = new char[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = ((string) values[i])[0];
			}
			return result;
		}
		case 'b':// signed char -> sbyte[]
		{
			var result = new sbyte[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToSByte(values[i]);
			}
			return result;
		}
		case 'B':// unsigned char -> byte[]
		{
			var result = new byte[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToByte(values[i]);
			}
			return result;
		}
		case 'h':// signed short -> short[]
		{
			var result = new short[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToInt16(values[i]);
			}
			return result;
		}
		case 'H':// unsigned short -> ushort[]
		{
			var result = new ushort[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToUInt16(values[i]);
			}
			return result;
		}
		case 'i':// signed integer -> int[]
		{
			var result = new int[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToInt32(values[i]);
			}
			return result;
		}
		case 'I':// unsigned integer 4 -> uint[]
		{
			var result = new uint[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToUInt32(values[i]);
			}
			return result;
		}
		case 'l':// signed long -> long[]
		{
			var result = new long[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToInt64(values[i]);
			}
			return result;
		}
		case 'L':// unsigned long -> ulong[]
		{
			var result = new ulong[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToUInt64(values[i]);
			}
			return result;
		}
		case 'f':// floating point 4 -> float[]
		{
			var result = new float[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToSingle(values[i]);
			}
			return result;
		}
		case 'd':// floating point 8 -> double[]
		{
			var result = new double[values.Count];
            for (int i = 0; i < result.Length; i++) {
				result[i] = Convert.ToDouble(values[i]);
			}
			return result;
		}
		default:
			throw new PickleException("invalid array typecode: " + typecode);
		}
	}

	/**
	 * Create an object based on machine code type
	 */
	public object construct(char typecode, int machinecode, byte[] data) {
		// Machine format codes.
		// Search for "enum machine_format_code" in Modules/arraymodule.c to get
		// the authoritative values.
		// UNKNOWN_FORMAT = -1
		// UNSIGNED_INT8 = 0
		// SIGNED_INT8 = 1
		// UNSIGNED_INT16_LE = 2
		// UNSIGNED_INT16_BE = 3
		// SIGNED_INT16_LE = 4
		// SIGNED_INT16_BE = 5
		// UNSIGNED_INT32_LE = 6
		// UNSIGNED_INT32_BE = 7
		// SIGNED_INT32_LE = 8
		// SIGNED_INT32_BE = 9
		// UNSIGNED_INT64_LE = 10
		// UNSIGNED_INT64_BE = 11
		// SIGNED_INT64_LE = 12
		// SIGNED_INT64_BE = 13
		// IEEE_754_FLOAT_LE = 14
		// IEEE_754_FLOAT_BE = 15
		// IEEE_754_DOUBLE_LE = 16
		// IEEE_754_DOUBLE_BE = 17
		// UTF16_LE = 18
		// UTF16_BE = 19
		// UTF32_LE = 20
		// UTF32_BE = 21

		if (machinecode < 0)
			throw new PickleException("unknown machine type format");

		switch (typecode) {
		case 'c':// character 1 -> char[]
		case 'u':// Unicode character 2 -> char[]
		{
			if (machinecode != 18 && machinecode != 19 && machinecode != 20 && machinecode != 21)
				throw new PickleException("for c/u type must be 18/19/20/21");
			if (machinecode == 18 || machinecode == 19) {
				// utf-16 , 2 bytes
				if (data.Length % 2 != 0)
					throw new PickleException("data size alignment error");
				return constructCharArrayUTF16(machinecode, data);
			}

			// utf-32, 4 bytes
			if (data.Length % 4 != 0)
				throw new PickleException("data size alignment error");
			return constructCharArrayUTF32(machinecode, data);
		}
		case 'b':// signed integer 1 -> sbyte[]
		{
			if (machinecode != 1)
				throw new PickleException("for b type must be 1");
			var result=new sbyte[data.Length];
			Buffer.BlockCopy(data,0,result,0,data.Length);
			return result;
		}
		case 'B':// unsigned integer 1 -> byte[]
		{
			if (machinecode != 0)
				throw new PickleException("for B type must be 0");
			return data;
		}
		case 'h':// signed integer 2 -> short[]
		{
			if (machinecode != 4 && machinecode != 5)
				throw new PickleException("for h type must be 4/5");
			if (data.Length % 2 != 0)
				throw new PickleException("data size alignment error");
			return constructShortArraySigned(machinecode, data);
		}
		case 'H':// unsigned integer 2 -> ushort[]
		{
			if (machinecode != 2 && machinecode != 3)
				throw new PickleException("for H type must be 2/3");
			if (data.Length % 2 != 0)
				throw new PickleException("data size alignment error");
			return constructUShortArrayFromUShort(machinecode, data);
		}
		case 'i':// signed integer 4 -> int[]
		{
			if (machinecode != 8 && machinecode != 9)
				throw new PickleException("for i type must be 8/9");
			if (data.Length % 4 != 0)
				throw new PickleException("data size alignment error");
			return constructIntArrayFromInt32(machinecode, data);
		}
		case 'l':// signed integer 4/8 -> int[]/long[]
		{
			if (machinecode != 8 && machinecode != 9 && machinecode != 12 && machinecode != 13)
				throw new PickleException("for l type must be 8/9/12/13");
			if ((machinecode==8 || machinecode==9) && data.Length % 4 != 0)
				throw new PickleException("data size alignment error");
			if ((machinecode==12 || machinecode==13) && data.Length % 8 != 0)
				throw new PickleException("data size alignment error");
			if(machinecode==8 || machinecode==9) {
				//32 bits
				return constructIntArrayFromInt32(machinecode, data);
			}

			//64 bits
			return constructLongArrayFromInt64(machinecode, data);
		}
		case 'I':// unsigned integer 4 -> uint[]
		{
			if (machinecode != 6 && machinecode != 7)
				throw new PickleException("for I type must be 6/7");
			if (data.Length % 4 != 0)
				throw new PickleException("data size alignment error");
			return constructUIntArrayFromUInt32(machinecode, data);
		}
		case 'L':// unsigned integer 4/8 -> uint[]/ulong[]
		{
			if (machinecode != 6 && machinecode != 7 && machinecode != 10 && machinecode != 11)
				throw new PickleException("for L type must be 6/7/10/11");
			if ((machinecode==6 || machinecode==7) && data.Length % 4 != 0)
				throw new PickleException("data size alignment error");
			if ((machinecode==10 || machinecode==11) && data.Length % 8 != 0)
				throw new PickleException("data size alignment error");
			if(machinecode==6 || machinecode==7) {
			    // 32 bits
			    return constructUIntArrayFromUInt32(machinecode, data);
			}

			// 64 bits
			return constructULongArrayFromUInt64(machinecode, data);
		}
		case 'f':// floating point 4 -> float[]
		{
			if (machinecode != 14 && machinecode != 15)
				throw new PickleException("for f type must be 14/15");
			if (data.Length % 4 != 0)
				throw new PickleException("data size alignment error");
			return constructFloatArray(machinecode, data);
		}
		case 'd':// floating point 8 -> double[]
		{
			if (machinecode != 16 && machinecode != 17)
				throw new PickleException("for d type must be 16/17");
			if (data.Length % 8 != 0)
				throw new PickleException("data size alignment error");
			return constructDoubleArray(machinecode, data);
		}
		default:
			throw new PickleException("invalid array typecode: " + typecode);
		}
	}

	protected int[] constructIntArrayFromInt32(int machinecode, byte[] data) {
		var result = new int[data.Length/4];
        if (machinecode == 8) {
            for (int i = 0; i < result.Length; i++) {
                result[i] = PickleUtils.bytes_to_integer(data, i * 4, 4);
            }
        }
        else
        {
		    var bigendian = new byte[4];
            for (int i = 0; i < result.Length; i++) {
                // big endian, swap
                bigendian[0] = data[3 + i * 4];
                bigendian[1] = data[2 + i * 4];
                bigendian[2] = data[1 + i * 4];
                bigendian[3] = data[0 + i * 4];
                result[i] = PickleUtils.bytes_to_integer(bigendian);
            }
        }
		return result;
	}

	protected uint[] constructUIntArrayFromUInt32(int machinecode, byte[] data) {
		var result=new uint[data.Length/4];
        if (machinecode == 6) {
            for (int i = 0; i < result.Length; i++) {
				result[i] = PickleUtils.bytes_to_uint(data, i*4);
            }
        }
        else
        {
		    var bigendian = new byte[4];
            for (int i = 0; i < result.Length; i++) {
				// big endian, swap
				bigendian[0]=data[3+i*4];
				bigendian[1]=data[2+i*4];
				bigendian[2]=data[1+i*4];
				bigendian[3]=data[0+i*4];
				result[i]=PickleUtils.bytes_to_uint(bigendian, 0);
            }
        }
		return result;
	}

	protected ulong[] constructULongArrayFromUInt64(int machinecode, byte[] data) {
		var result = new ulong[data.Length/8];
        if (BitConverter.IsLittleEndian) {
            if (machinecode == 10) {
                for (int i = 0; i < result.Length; i++) {
                    result[i] = BitConverter.ToUInt64(data, i * 8);
                }
            } else {
                var swapped = new byte[8];
                for (int i = 0; i < result.Length; i++) {
                    swapped[0] = data[7 + i * 8];
                    swapped[1] = data[6 + i * 8];
                    swapped[2] = data[5 + i * 8];
                    swapped[3] = data[4 + i * 8];
                    swapped[4] = data[3 + i * 8];
                    swapped[5] = data[2 + i * 8];
                    swapped[6] = data[1 + i * 8];
                    swapped[7] = data[0 + i * 8];
                    result[i] = BitConverter.ToUInt64(swapped, 0);
                }
            }
        } else {
            if (machinecode == 11) {
                for (int i = 0; i < result.Length; i++) {
                    result[i] = BitConverter.ToUInt64(data, i * 8);
                }
            }
            else
            {
                var swapped = new byte[8];
                for (int i = 0; i < result.Length; i++) {
                    swapped[0] = data[7 + i * 8];
                    swapped[1] = data[6 + i * 8];
                    swapped[2] = data[5 + i * 8];
                    swapped[3] = data[4 + i * 8];
                    swapped[4] = data[3 + i * 8];
                    swapped[5] = data[2 + i * 8];
                    swapped[6] = data[1 + i * 8];
                    swapped[7] = data[0 + i * 8];
                    result[i] = BitConverter.ToUInt64(swapped, 0);
                }
            }
        }
		return result;
	}

	protected long[] constructLongArrayFromInt64(int machinecode, byte[] data) {
		var result=new long[data.Length/8];
        if (machinecode == 12) {
            for (int i = 0; i < result.Length; i++) {
				result[i]=PickleUtils.bytes_to_long(data, i*8);
            }
        }
        else { 
		    var bigendian=new byte[8];
            for (int i = 0; i < result.Length; i++) {
				// 13=big endian, swap
				bigendian[0]=data[7+i*8];
				bigendian[1]=data[6+i*8];
				bigendian[2]=data[5+i*8];
				bigendian[3]=data[4+i*8];
				bigendian[4]=data[3+i*8];
				bigendian[5]=data[2+i*8];
				bigendian[6]=data[1+i*8];
				bigendian[7]=data[0+i*8];
				result[i]=PickleUtils.bytes_to_long(bigendian, 0);
			}
		}
		return result;
	}	

	protected double[] constructDoubleArray(int machinecode, byte[] data) {
		var result = new double[data.Length / 8];
        if (machinecode == 17) {
            for (int i = 0; i < result.Length; i++) {
                result[i] = PickleUtils.bytes_bigendian_to_double(data, i * 8);
            }
        } else {
		    var bigendian=new byte[8];
            for (int i = 0; i < result.Length; i++) {
                // 16=little endian, flip the bytes
				bigendian[0]=data[7+i*8];
				bigendian[1]=data[6+i*8];
				bigendian[2]=data[5+i*8];
				bigendian[3]=data[4+i*8];
				bigendian[4]=data[3+i*8];
				bigendian[5]=data[2+i*8];
				bigendian[6]=data[1+i*8];
				bigendian[7]=data[0+i*8];
				result[i] = PickleUtils.bytes_bigendian_to_double(bigendian, 0);
            }
        }
		return result;
	}

	protected float[] constructFloatArray(int machinecode, byte[] data) {
		var result = new float[data.Length / 4];
        if (machinecode == 15) {
            for (int i = 0; i < result.Length; i++) {
                result[i] = PickleUtils.bytes_bigendian_to_float(data, i * 4);
            }
        } else {
		    var bigendian=new byte[4];
            for (int i = 0; i < result.Length; i++) {
				// 14=little endian, flip the bytes
				bigendian[0]=data[3+i*4];
				bigendian[1]=data[2+i*4];
				bigendian[2]=data[1+i*4];
				bigendian[3]=data[0+i*4];
				result[i] = PickleUtils.bytes_bigendian_to_float(bigendian, 0);
            }
        }
		return result;
	}

	protected ushort[] constructUShortArrayFromUShort(int machinecode, byte[] data) {
		var result = new ushort[data.Length / 2];
        if (machinecode == 2) {
            for (int i = 0; i < result.Length; i++) {
                ushort b1=data[0+i*2];
			    ushort b2=data[1+i*2];
			    result[i] = (ushort)((b2<<8) | b1);
            }
        } else {
            for (int i = 0; i < result.Length; i++) {
                ushort b1=data[0+i*2];
			    ushort b2=data[1+i*2];
				// big endian
				result[i] = (ushort)((b1<<8) | b2);
            }
        }
		return result;
	}

	protected short[] constructShortArraySigned(int machinecode, byte[] data) {
		var result = new short[data.Length / 2];
		if(BitConverter.IsLittleEndian) {
            if (machinecode == 4) { 
                for (int i = 0; i < result.Length; i++) {
		            result[i]=BitConverter.ToInt16(data, i*2);
                }
            } else {
		        var swapped=new byte[2];
                for (int i = 0; i < result.Length; i++) {
                    swapped[0] = data[1+i*2];
		            swapped[1]=data[0+i*2];
    			    result[i]=BitConverter.ToInt16(swapped,0);
                }
            }
		} else {
            if (machinecode == 5) { 
                for (int i = 0; i < result.Length; i++) {
    			    result[i]=BitConverter.ToInt16(data, i*2);
                }
            } else {
		        var swapped=new byte[2];
                for (int i = 0; i < result.Length; i++) {
                    swapped[0]=data[1+i*2];
		            swapped[1]=data[0+i*2];
    			    result[i]=BitConverter.ToInt16(swapped,0);
                }
    		}
		}
		return result;
	}

	protected char[] constructCharArrayUTF32(int machinecode, byte[] data) {
		var result = new char[data.Length / 4];
        if (machinecode == 20) {
            for (int index = 0; index < result.Length; ++index) {
		        int codepoint=PickleUtils.bytes_to_integer(data, index*4, 4);
		        string cc=char.ConvertFromUtf32(codepoint);
                if (cc.Length>1)
					throw new PickleException("cannot process UTF-32 character codepoint "+codepoint);
		        result[index] = cc[0];
            }
        } else {
		    var bigendian=new byte[4];
            for (int index = 0; index < result.Length; ++index) {
                // big endian, swap
				bigendian[0]=data[3+index*4];
				bigendian[1]=data[2+index*4];
				bigendian[2]=data[1+index*4];
				bigendian[3]=data[index*4];
				int codepoint=PickleUtils.bytes_to_integer(bigendian);
				string cc=char.ConvertFromUtf32(codepoint);
				if(cc.Length>1)
					throw new PickleException("cannot process UTF-32 character codepoint "+codepoint);
				result[index] = cc[0];
            }
        }
		return result;
	}

	protected char[] constructCharArrayUTF16(int machinecode, byte[] data) {
		var result = new char[data.Length / 2];
        if (machinecode == 18) {
		    for (int index = 0; index < result.Length; ++index) {
				result[index] = (char) PickleUtils.bytes_to_integer(data, index*2, 2);
            }
        } else {
		    var bigendian=new byte[2];
		    for (int index = 0; index < result.Length; ++index) {
                // big endian, swap
				bigendian[0]=data[1+index*2];
				bigendian[1]=data[0+index*2];
				result[index] = (char) PickleUtils.bytes_to_integer(bigendian);
            }
        }
		return result;
	}    
}

}
