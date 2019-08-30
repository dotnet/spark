using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    public sealed class Function : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Function(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public string Database => (string)_jvmObject.Invoke("database");

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public string Description => (string)_jvmObject.Invoke("description");

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public bool IsTemporary => (bool)_jvmObject.Invoke("isTemporary");

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public string Name => (string)_jvmObject.Invoke("name");

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public string ClassName => (string)_jvmObject.Invoke("className");

        /// <summary>
        /// 
        /// </summary>
        /// <returns>string</returns>
        public new string ToString => (string)_jvmObject.Invoke("toString");
    }
}
