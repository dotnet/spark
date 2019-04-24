// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;

namespace Razorvine.Pickle.Objects
{

/// <summary>
/// This object constructor uses reflection to create instances of any given class. 
/// </summary>
internal class AnyClassConstructor : IObjectConstructor {

	private readonly Type _type;

	public AnyClassConstructor(Type type) {
		_type = type;
	}

	public object construct(object[] args) {
		try {
			return Activator.CreateInstance(_type, args);
		} catch (Exception x) {
			throw new PickleException("problem constructing object",x);
		}
	}
}

}
