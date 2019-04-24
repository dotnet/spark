// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

// ReSharper disable InconsistentNaming
namespace Razorvine.Pickle
{

/// <summary>
/// Interface for object Constructors that are used by the unpickler
/// to create instances of non-primitive or custom classes.
/// </summary>
internal interface IObjectConstructor {

	/**
	 * Create an object. Use the given args as parameters for the constructor.
	 */
	object construct(object[] args);
}

}
