// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

namespace Razorvine.Pickle
{
	/// <summary>
	/// Exception thrown when the unpickler encountered an unknown or unimplemented opcode.
	/// </summary>
	internal class InvalidOpcodeException : PickleException
	{
		public InvalidOpcodeException(string message) : base(message)
		{
		}
	}
}
