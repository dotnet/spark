using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    public sealed class Database : IJvmObjectReferenceProvider
    {
        private JvmObjectReference _jvmObject;

        internal Database(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }
        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

  
        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public string Description => (string)_jvmObject.Invoke("description");

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public string LocationUri => (string)_jvmObject.Invoke("locationUri");

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public string Name => (string)_jvmObject.Invoke("name");

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public new string ToString => (string)_jvmObject.Invoke("toString");
    }
}