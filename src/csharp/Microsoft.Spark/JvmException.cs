using System;

namespace Microsoft.Spark
{
    /// <summary>
    /// Contains the message returned from the <see cref="Interop.Ipc.JvmBridge"/> on an error.
    /// </summary>
    public class JvmException : Exception
    {
        public JvmException(string message) 
            : base(message)
        {
        }
    }
}
