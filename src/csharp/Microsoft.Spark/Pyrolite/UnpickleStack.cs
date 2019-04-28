// Licensed to the .NET Foundation under one or more agreements.
// See the LICENSE file in the project root for more information.

/* part of Pyrolite, by Irmen de Jong (irmen@razorvine.net) */

using System;
using System.Collections;
using System.Diagnostics.CodeAnalysis;

namespace Razorvine.Pickle
{

/// <summary>
/// Helper type that represents the unpickler working stack. 
/// </summary>
[SuppressMessage("ReSharper", "InconsistentNaming")]
internal class UnpickleStack {
	public readonly object MARKER;
    private const int DefaultCapacity = 128;
	private object[] _stack;
    private int _count;

	public UnpickleStack() {
		_stack = new object[DefaultCapacity];
		MARKER = new object(); // any new unique object
	}

    public void add(object o)
    {
        if (_count == _stack.Length)
        {
            Array.Resize(ref _stack, _count * 2);
        }

        _stack[_count++] = o;
    }

	public void add_mark() {
		add(MARKER);
	}

    public object pop()
    {
        if (_count == 0)
        {
            throw new ArgumentOutOfRangeException("index"); // match exception type/parameter name thrown from ArrayList used previously
        }

        return _stack[--_count];
    }

	public ArrayList pop_all_since_marker() {
		var result = new ArrayList();
		object o = pop();
		while (o != MARKER) {
			result.Add(o);
			o = pop();
		}
		result.TrimToSize();
		result.Reverse();
		return result;
	}

    internal object[] pop_all_since_marker_as_array()
    {
        int i = _count - 1;
        while (_stack[i] != MARKER)
            i--;

        var result = new object[_count - 1 - i];
        Array.Copy(_stack, i + 1, result, 0, result.Length);

        _count = i;
        return result;
    }

	public object peek() {
        return _stack[_count - 1];
    }

	public void trim() {
        if (_count < _stack.Length && _stack.Length > DefaultCapacity)
        {
            var newArr = new object[Math.Max(_count, DefaultCapacity)];
            Array.Copy(_stack, 0, newArr, 0, _count);
            _stack = newArr;
        }
	}

	public int size() {
		return _count;
	}

	public void clear() {
        _count = 0;
		trim();
	}
}

}
