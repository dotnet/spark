// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Text;
// ReSharper disable InconsistentNaming
// ReSharper disable InvertIf

namespace Razorvine.Pickle
{
	
/// <summary>
/// Utility stuff for dealing with pickle data streams. 
/// </summary>
internal static class PickleUtils {

	/**
	 * read a line of text, possibly including the terminating LF char
	 */
	public static string readline(Stream input, bool includeLF = false) {
		StringBuilder sb = new StringBuilder();
		while (true) {
			int c = input.ReadByte();
			if (c == -1) {
                if (sb.Length == 0)
                    ThrowPrematureEndOfInputStream();
                break;
			}
			if (c != '\n' || includeLF)
				sb.Append((char) c);
			if (c == '\n')
				break;
		}
		return sb.ToString();
	}

    internal static int readline_into(Stream input, ref byte[] buffer, bool includeLF = false)
    {
        Debug.Assert(buffer != null);
        byte[] localBuffer = buffer;

        int count = 0;
		while (true) {
			int c = input.ReadByte();
			if (c == -1) {
				if (count == 0) ThrowPrematureEndOfInputStream();
				break;
			}
			if (c != '\n' || includeLF)
            {
                if (count == localBuffer.Length)
                {
                    Array.Resize(ref localBuffer, localBuffer.Length == 0 ? 16 : localBuffer.Length * 2);
                    buffer = localBuffer;
                }
                localBuffer[count++] = (byte)c;
            }
			if (c == '\n')
			    break;
		}

		return count;
    }

    internal static bool IsWhitespace(ReadOnlySpan<byte> bytes)
    {
        for (int i = 0; i < bytes.Length; i++)
        {
            if (!char.IsWhiteSpace((char)bytes[i]))
                return false;
        }
        return true;
    }

	/**
	 * read a single unsigned byte
	 */
	public static byte readbyte(Stream input) {
		int b = input.ReadByte();
		if(b<0) {
			ThrowPrematureEndOfInputStream();
		}
		return (byte)b;
	}

    private static void ThrowPrematureEndOfInputStream() =>
        throw new IOException("premature end of input stream");

	/**
	 * read a number of signed bytes
	 */
	public static byte[] readbytes(Stream input, int n) {
		var buffer = new byte[n];
		readbytes_into(input, buffer, 0, n);
		return buffer;
	}

	/**
	 * read a number of signed bytes
	 */
	public static byte[] readbytes(Stream input, long n) {
		try {
			return readbytes(input, checked((int)n));
		} catch (OverflowException x) {
			throw new PickleException("pickle too large, can't read more than maxint", x);
		}
	}

	/**
	 * read a number of signed bytes into the specified location in an existing byte array
	 */
	public static void readbytes_into(Stream input, byte[] buffer, int offset, int length) {
		while (length > 0) {
			int read = input.Read(buffer, offset, length);
			if (read <= 0)
                ThrowPrematureEndOfInputStream();
            offset += read;
			length -= read;
		}
	}

    /**
	 * read a number of signed bytes into the specified location in an existing byte array
	 */
	internal static void readbytes_into(Stream input, byte[] buffer, int offset, long length) {
        try {
			readbytes_into(input, buffer, offset, checked((int)length));
		} catch (OverflowException x) {
			throw new PickleException("pickle too large, can't read more than maxint", x);
		}
	}

	/**
	 * Convert a couple of bytes into the corresponding integer number.
	 * Can deal with 2-bytes unsigned int and 4-bytes signed int.
	 */
	public static int bytes_to_integer(byte[] bytes) {
		return bytes_to_integer(bytes, 0, bytes.Length);
	}

    /**
	 * Convert a couple of bytes into the corresponding integer number.
	 * Can deal with 2-bytes unsigned int and 4-bytes signed int.
	 */
	public static int bytes_to_integer(byte[] bytes, int offset, int size)
	{
		// this operates on little-endian bytes

		switch (size)
		{
			case 2:
				// 2-bytes unsigned int
				if (BitConverter.IsLittleEndian) return BitConverter.ToUInt16(bytes, offset);
				// need to byteswap because the converter needs big-endian...
				return BitConverter.ToUInt16(new[] {bytes[1+offset], bytes[0+offset]}, 0);
			case 4:
				// 4-bytes signed int
				if (BitConverter.IsLittleEndian) return BitConverter.ToInt32(bytes, offset);
				// need to byteswap because the converter needs big-endian...
				return BitConverter.ToInt32(new[] {bytes[3+offset], bytes[2+offset], bytes[1+offset], bytes[0+offset]}, 0);
			default:
				throw new PickleException("invalid amount of bytes to convert to int: " + size);
		}
	}

	/**
	 * Convert 8 little endian bytes into a long
	 */
	public static long bytes_to_long(byte[] bytes, int offset) {
		if(bytes.Length-offset<8)
			throw new PickleException("too few bytes to convert to long");
		if(BitConverter.IsLittleEndian) {
    		return BitConverter.ToInt64(bytes, offset);
		}
		// need to byteswap because the converter needs big-endian...
		byte[] bigendian={bytes[7+offset], bytes[6+offset], bytes[5+offset], bytes[4+offset], bytes[3+offset], bytes[2+offset], bytes[1+offset], bytes[0+offset]};
		return BitConverter.ToInt64(bigendian, 0);
	}	

	/**
	 * Convert 4 little endian bytes into an unsigned int
	 */
	public static uint bytes_to_uint(byte[] bytes, int offset) {
		if(bytes.Length-offset<4)
			throw new PickleException("too few bytes to convert to long");
		if(BitConverter.IsLittleEndian) {
    		return BitConverter.ToUInt32(bytes, offset);
		}
		// need to byteswap because the converter needs big-endian...
		byte[] bigendian={bytes[3+offset], bytes[2+offset], bytes[1+offset], bytes[0+offset]};
		return BitConverter.ToUInt32(bigendian, 0);
	}	
		
	/**
	 * Convert a signed integer to its 4-byte representation. (little endian)
	 */
	public static byte[] integer_to_bytes(int i) {
		var bytes=BitConverter.GetBytes(i);
		if(!BitConverter.IsLittleEndian) {
			// reverse the bytes to make them little endian
			Array.Reverse(bytes);
		}
		return bytes;
	}

	/**
	 * Convert a double to its 8-byte representation (big endian).
	 */
	public static byte[] double_to_bytes_bigendian(double d) {
		var bytes=BitConverter.GetBytes(d);
		if(BitConverter.IsLittleEndian) {
			// reverse the bytes to make them big endian for the output
			Array.Reverse(bytes);
		}
		return bytes;
	}

	/**
	 * Convert a big endian 8-byte to a double. 
	 */
	public static double bytes_bigendian_to_double(byte[] bytes, int offset) {
		if (bytes.Length-offset<8) {
			throw new PickleException("decoding double: too few bytes");
	    }
    	if(BitConverter.IsLittleEndian) {
            return BitConverter.Int64BitsToDouble(BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan(offset)));
		}
		return BitConverter.ToDouble(bytes,offset);
	}

	/**
	 * Convert a big endian 4-byte to a float. 
	 */
	public static float bytes_bigendian_to_float(byte[] bytes, int offset) {
		if (bytes.Length-offset<4) {
			throw new PickleException("decoding float: too few bytes");
	    }
    	if(BitConverter.IsLittleEndian) {
            int intVal = BinaryPrimitives.ReadInt32BigEndian(bytes.AsSpan(offset));
            unsafe { return *(float*)&intVal; };
		}
		return BitConverter.ToSingle(bytes,offset);
	}
	
	/**
	 * read an arbitrary 'long' number. 
	 * because c# doesn't have a bigint, we look if stuff fits into a regular long,
	 * and raise an exception if it's bigger.
	 */
	public static long decode_long(byte[] data) {
		if (data.Length == 0)
			return 0L;
		if (data.Length>8)
			throw new PickleException("value too large for long, biginteger needed");
		if( data.Length<8) {
			// bitconverter requires exactly 8 bytes so we need to extend it
			var larger=new byte[8];
			Array.Copy(data,larger,data.Length);
			
			// check if we need to sign-extend (if the original number was negative)
			if((data[data.Length-1]&0x80) == 0x80) {
				for(int i=data.Length; i<8; ++i) {
					larger[i]=0xff;
				}
			}
			data=larger;
		}
		if(!BitConverter.IsLittleEndian) {
			// reverse the byte array because pickle stores it little-endian	
			Array.Reverse(data);
		}
		return BitConverter.ToInt64(data,0);
	}
	
	/**
	 * Construct a string from the given bytes where these are directly
	 * converted to the corresponding chars, without using a given character
	 * encoding
	 */
	public static string rawStringFromBytes(byte[] data)
    {
        return rawStringFromBytes(new ReadOnlySpan<byte>(data));
    }

    internal static unsafe string rawStringFromBytes(ReadOnlySpan<byte> data) {
        var result = new string('\0', data.Length); // Use String.Create instead when it's available
        fixed (char* resultPtr = result)
        {
            for (int i = 0; i < data.Length; i++)
                resultPtr[i] = (char)data[i];
        }
        return result;
	}
	
	/**
	 * Convert a string to a byte array, no encoding is used. String must only contain characters &lt; 256.
	 */
	public static byte[] str2bytes(string str)  {
		var b=new byte[str.Length];
		for(int i=0; i<str.Length; ++i) {
			char c=str[i];
			if(c>255) throw new ArgumentException("string contained a char > 255, cannot convert to bytes");
			b[i]=(byte)c;
		}
		return b;
	}	

	/**
	 * Decode a string with possible escaped char sequences in it (\x??).
	 */
	public static string decode_escaped(string str) {
		if(str.IndexOf('\\')==-1)
			return str;
		StringBuilder sb=new StringBuilder(str.Length);
		for(int i=0; i<str.Length; ++i) {
			char c=str[i];
			if(c=='\\') {
				// possible escape sequence
				char c2=str[++i];
				switch(c2) {
					case '\\':
						// double-escaped '\\'--> '\'
						sb.Append(c);
						break;
					case 'x':
						// hex escaped "\x??"
						char h1=str[++i];
						char h2=str[++i];
						c2=(char)Convert.ToInt32(""+h1+h2, 16);
						sb.Append(c2);
						break;
					case 'n':
						sb.Append('\n');
						break;
					case 'r':
						sb.Append('\r');
						break;
					case 't':
						sb.Append('\t');
						break;
					case '\'':
						sb.Append('\'');   			// sometimes occurs in protocol level 0 strings
						break;
					default:
						if(str.Length>80)
							str=str.Substring(0, 80);
						throw new PickleException("invalid escape sequence char \'"+c2+"\' in string \""+str+" [...]\" (possibly truncated)");
				}
			} else {
				sb.Append(str[i]);
			}
		}
        return sb.ToString();
	}

	/**
	 * Decode a string with possible escaped unicode in it (\u20ac)
	 */
	public static string decode_unicode_escaped(string str) {
		if(str.IndexOf('\\')==-1)
			return str;
		StringBuilder sb=new StringBuilder(str.Length);
		for(int i=0; i<str.Length; ++i) {
			char c=str[i];
			if(c=='\\') {
				// possible escape sequence
				char c2=str[++i];
				switch(c2) {
					case '\\':
						// double-escaped '\\'--> '\'
						sb.Append(c);
						break;
					case 'u':
						// hex escaped unicode "\u20ac"
						char h1=str[++i];
						char h2=str[++i];
						char h3=str[++i];
						char h4=str[++i];
						c2=(char)Convert.ToInt32(""+h1+h2+h3+h4, 16);
						sb.Append(c2);
						break;
					case 'n':
						sb.Append('\n');
						break;
					case 'r':
						sb.Append('\r');
						break;
					case 't':
						sb.Append('\t');
						break;
					default:
						if(str.Length>80)
							str=str.Substring(0, 80);
						throw new PickleException("invalid escape sequence char \'"+c2+"\' in string \""+str+" [...]\" (possibly truncated)");
				}
			} else {
				sb.Append(str[i]);
			}
		}
        return sb.ToString();
	}
}

}
