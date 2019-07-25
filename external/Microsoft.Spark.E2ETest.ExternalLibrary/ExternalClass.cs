using System;

namespace Microsoft.Spark.E2ETest.ExternalLibrary
{
    [Serializable]
    public class ExternalClass
    {
        private string _s;

        public ExternalClass(string s)
        {
            _s = s;
        }

        public static void HelloWorld()
        {
            Console.WriteLine("Hello World");
        }

        public string Concat(string s)
        {
            return _s + s;
        }
    }
}
