// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Interop.Scala
{
    /// <summary>
    /// Exposes subset of scala.Option[T] APIs.
    /// </summary>
    internal sealed class Option : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Option(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Returns true if the option is None, false otherwise.
        /// </summary>
        /// <returns>true if the option is None, false otherwise</returns>
        internal bool IsEmpty() => (bool)_jvmObject.Invoke("isEmpty");

        /// <summary>
        /// Returns true if the option is an instance of Some, false otherwise.
        /// </summary>
        /// <returns>true if the option is an instance of Some, false otherwise</returns>
        internal bool IsDefined() => (bool)_jvmObject.Invoke("isDefined");

        /// <summary>
        /// Returns the option's value as object type if the option is nonempty,
        /// otherwise throws an exception on JVM side.
        /// </summary>
        /// <returns>object that this Option is referencing to</returns>
        internal object Get() => _jvmObject.Invoke("get");

        /// <summary>
        /// Returns the option's value if it is nonempty, or `null` if it is empty.
        /// </summary>
        /// <returns>object that this Option is referencing to</returns>
        internal object OrNull() => IsDefined() ? Get() : null;
    }
}
