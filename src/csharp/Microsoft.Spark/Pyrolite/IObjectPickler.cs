// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System.IO;
// ReSharper disable InconsistentNaming

namespace Razorvine.Pickle
{
	
/// <summary>
/// Interface for object Picklers used by the pickler, to pickle custom classes. 
/// </summary>
internal interface IObjectPickler {
	/**
	 * Pickle an object.
	 */
	void pickle(object o, Stream outs, Pickler currentPickler);
}

}
