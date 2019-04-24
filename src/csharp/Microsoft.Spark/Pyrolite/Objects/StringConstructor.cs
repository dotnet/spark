// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

// ReSharper disable UnusedMember.Global
namespace Razorvine.Pickle.Objects
{

/// <summary>
/// This object constructor uses reflection to create instances of the string type.
/// AnyClassConstructor cannot be used because string doesn't have the appropriate constructors.
///	see http://stackoverflow.com/questions/2092530/how-do-i-use-activator-createinstance-with-strings
/// </summary>
internal class StringConstructor : IObjectConstructor
{
	public object construct(object[] args)
	{
		if(args.Length==0) {
			return "";
		}

		if(args.Length==1 && args[0] is string) {
			return (string)args[0];
		}

		throw new PickleException("invalid string constructor arguments");
	}
}

}
