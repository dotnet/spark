using System;

namespace Microsoft.Spark
{
    /// <summary>
    /// Contains the message returned from the Jvm Bridge on an error
    /// </summary>
    public class JvmException : Exception
    {
        public JvmException(string message) 
            : base(message) { }
    }
}
