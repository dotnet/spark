// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
using System.Collections.Generic;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Interop.Internal.Scala
{
    /// <summary>
    /// Limited read-only implementation of Scala Seq[T] so that Seq objects can be read
    /// into POCO collection types such as List.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class Seq<T> : IJvmObjectReferenceProvider, IEnumerable<T>
    {
        internal Seq(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public int Size => (int)Reference.Invoke("size");

        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < Size; ++i)
            {
                yield return Apply(i);
            }
        }

        public T Apply(int index) => (T)Reference.Invoke("apply", index);
    }
}
