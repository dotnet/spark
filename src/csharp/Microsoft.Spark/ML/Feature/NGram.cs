using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.ML.Feature
{
    public class NGram : FeatureBase<NGram>, IJvmObjectReferenceProvider
    {
        private static readonly string s_NGramClassName =
            "org.apache.spark.ml.feature.NGram";

        public NGram() : base(s_NGramClassName)
        {
        }

        public NGram(string uid) : base(s_NGramClassName, uid)
        {
        }

        internal NGram(JvmObjectReference jvmObject) : base(jvmObject)
        {
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;
    }
}
