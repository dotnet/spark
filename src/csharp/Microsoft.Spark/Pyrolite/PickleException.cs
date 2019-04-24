// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;

namespace Razorvine.Pickle
{
    /// <summary>
    /// Exception thrown when something went wrong with pickling or unpickling.
    /// </summary>
    internal class PickleException : Exception
	{
		public PickleException(string message) : base(message)
		{
		}

		public PickleException(string message, Exception innerException) : base(message, innerException)
		{
		}
	}
}
