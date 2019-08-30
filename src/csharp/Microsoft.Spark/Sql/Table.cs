using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    public sealed class Table : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal Table(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
    }
}