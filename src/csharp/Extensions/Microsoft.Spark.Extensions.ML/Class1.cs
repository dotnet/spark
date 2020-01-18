using System;

namespace Microsoft.Spark.Extensions.ML
{
    public class Pipeline<T> where T : new()
    {
        public T Load(string path)
        {
            return new T();
        }
    }
}
