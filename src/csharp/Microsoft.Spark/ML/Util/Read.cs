// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// Class for utility classes that can load ML instances.
    /// </summary>
    /// <typeparam name="T">ML instance type</typeparam>
    public class JavaMLReader<T> : IJvmObjectReferenceProvider
    {
        internal JavaMLReader(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Loads the ML component from the input path.
        /// </summary>
        /// <param name="path">The path the previous instance of type T was saved to</param>
        /// <returns>The type T instance</returns>
        public T Load(string path) =>
            WrapAsType((JvmObjectReference)Reference.Invoke("load", path));

        /// <summary>Sets the Spark Session to use for saving/loading.</summary>
        /// <param name="sparkSession">The Spark Session to be set</param>
        public JavaMLReader<T> Session(SparkSession sparkSession)
        {
            Reference.Invoke("session", sparkSession);
            return this;
        }

        private static T WrapAsType(JvmObjectReference reference)
        {
            ConstructorInfo constructor = typeof(T)
                .GetConstructors(BindingFlags.NonPublic | BindingFlags.Instance)
                .Single(c =>
                {
                    ParameterInfo[] parameters = c.GetParameters();
                    return (parameters.Length == 1) &&
                        (parameters[0].ParameterType == typeof(JvmObjectReference));
                });

            return (T)constructor.Invoke(new object[] { reference });
        }
    }

    /// <summary>
    /// Interface for objects that provide MLReader.
    /// </summary>
    /// <typeparam name="T">
    /// ML instance type
    /// </typeparam>
    public interface IJavaMLReadable<T>
    {
        /// <summary>
        /// Get the corresponding JavaMLReader instance.
        /// </summary>
        /// <returns>an <see cref="JavaMLReader&lt;T&gt;"/> instance for this ML instance.</returns>
        JavaMLReader<T> Read();
    }
}
