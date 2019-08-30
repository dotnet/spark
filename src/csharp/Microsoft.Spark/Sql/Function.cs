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
    }
}