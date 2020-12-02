using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Extensions.FileSystem
{
    /// <summary>
    /// Basic .NET implementation of IFileSystem.
    /// </summary>
    public class FileSystem : IFileSystem, IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal FileSystem(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Returns the configured filesystem implementation.
        /// </summary>
        /// <param name="sparkContext">The SparkContext whose configuration will be used.</param>
        /// <returns>The FileSystem.</returns>
        public static IFileSystem Get(SparkContext sparkContext)
        {
            // TODO: Expose hadoopConfiguration as a .NET class and add an override for Get() that takes it.
            JvmObjectReference hadoopConfiguration = (JvmObjectReference)
                ((IJvmObjectReferenceProvider)sparkContext).Reference.Invoke("hadoopConfiguration");

            return new FileSystem(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    "org.apache.hadoop.fs.FileSystem",
                    "get",
                    hadoopConfiguration));
        }

        /// <summary>
        /// Delete a file.
        /// </summary>
        /// <param name="path">The path to delete.</param>
        /// <param name="recursive">If path is a directory and set to true, the directory is deleted else throws an
        /// exception. In case of a file the recursive can be set to either true or false.</param>
        /// <returns>True if delete is successful else false.</returns>
        public bool Delete(string path, bool recursive = true)
        {
            JvmObjectReference pathObject =
                SparkEnvironment.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);

            return (bool)_jvmObject.Invoke("delete", pathObject, recursive);
        }

        public void Dispose()
        {
            _jvmObject.Invoke("close");
        }
    }
}
