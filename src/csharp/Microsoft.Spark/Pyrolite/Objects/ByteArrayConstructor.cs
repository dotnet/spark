// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Collections;
using System.Text;

namespace Razorvine.Pickle.Objects
{

/// <summary>
/// Creates byte arrays (byte[]). 
/// </summary>
internal class ByteArrayConstructor : IObjectConstructor {

	public object construct(object[] args) {
		// args for bytearray constructor: [ String string, String encoding ]
		// args for bytearray constructor (from python3 bytes): [ ArrayList ] or just [byte[]] (when it uses BINBYTES opcode)
		if (args.Length != 1 && args.Length != 2)
			throw new PickleException("invalid pickle data for bytearray; expected 1 or 2 args, got "+args.Length);

		if(args.Length==1) {
			if(args[0] is byte[]) {
				return args[0];
			}
			ArrayList values=(ArrayList) args[0];
			var data=new byte[values.Count];
			for(int i=0; i<data.Length; ++i) {
				data[i] = Convert.ToByte(values[i]);
			}
			return data;
		} else {
			// This thing is fooling around with byte<>string mappings using an encoding.
			// I think that is fishy... but for now it seems what Python itself is also doing...
			string data = (string) args[0];
			string encoding = (string) args[1];
			if (encoding.StartsWith("latin-"))
				encoding = "ISO-8859-" + encoding.Substring(6);
			return Encoding.GetEncoding(encoding).GetBytes(data);
		}
	}
}

}
