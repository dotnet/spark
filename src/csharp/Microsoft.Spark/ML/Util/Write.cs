// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.ML.Feature
{
    /// <summary>
    /// Class for utility classes that can save ML instances in Spark's internal format.
    /// </summary>
    public class JavaMLWriter : IJvmObjectReferenceProvider
    {
        internal JavaMLWriter(JvmObjectReference jvmObject) => Reference = jvmObject;

        public JvmObjectReference Reference { get; private set; }

        /// <summary>Saves the ML instances to the input path.</summary>
        /// <param name="path">The path to save the object to</param>
        public void Save(string path) => Reference.Invoke("save", path);

        /// <summary>
        /// save() handles overwriting and then calls this method. 
        /// Subclasses should override this method to implement the actual saving of the instance.
        /// </summary>
        /// <param name="path">The path to save the object to</param>
        protected void SaveImpl(string path) => Reference.Invoke("saveImpl", path);

        /// <summary>Overwrites if the output path already exists.</summary>
        public JavaMLWriter Overwrite()
        {
            Reference.Invoke("overwrite");
            return this;
        }

        /// <summary>
        /// Adds an option to the underlying MLWriter. See the documentation for the specific model's
        /// writer for possible options. The option name (key) is case-insensitive.
        /// </summary>
        /// <param name="key">key of the option</param>
        /// <param name="value">value of the option</param>
        public JavaMLWriter Option(string key, string value)
        {
            Reference.Invoke("option", key, value);
            return this;
        }

        /// <summary>Sets the Spark Session to use for saving/loading.</summary>
        /// <param name="sparkSession">The Spark Session to be set</param>
        public JavaMLWriter Session(SparkSession sparkSession)
        {
            Reference.Invoke("session", sparkSession);
            return this;
        }
    }

    /// <summary>
    /// Interface for classes that provide JavaMLWriter.
    /// </summary>
    public interface IJavaMLWritable
    {
        /// <summary>
        /// Get the corresponding JavaMLWriter instance.
        /// </summary>
        /// <returns>a <see cref="JavaMLWriter"/> instance for this ML instance.</returns>
        JavaMLWriter Write();

        /// <summary>Saves this ML instance to the input path</summary>
        /// <param name="path">The path to save the object to</param>
        void Save(string path);
    }
}
