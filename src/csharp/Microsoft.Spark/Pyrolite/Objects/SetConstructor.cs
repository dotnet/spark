// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System.Collections;
using System.Collections.Generic;

namespace Razorvine.Pickle.Objects
{

/// <summary>
/// This object constructor creates sets. (HashSet&lt;object&gt;)
/// </summary>
internal class SetConstructor : IObjectConstructor {

	public object construct(object[] args) {
		// create a HashSet, args=arraylist of stuff to put in it
		ArrayList elements=(ArrayList)args[0];
		IEnumerable<object> array=elements.ToArray();
		return new HashSet<object>(array);
	}
}

}
