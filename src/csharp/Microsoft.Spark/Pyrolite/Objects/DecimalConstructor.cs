// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Globalization;

namespace Razorvine.Pickle.Objects
{

/// <summary>
/// This object constructor uses reflection to create instances of the decimal type.
/// (AnyClassConstructor cannot be used because decimal doesn't have the appropriate constructors).
/// </summary>
internal class DecimalConstructor : IObjectConstructor
{
	public object construct(object[] args)
	{
		if(args.Length==1 && args[0] is string) {
			return Convert.ToDecimal((string)args[0], CultureInfo.InvariantCulture);
		}

		throw new PickleException("invalid arguments for decimal constructor");
	}
}

}
