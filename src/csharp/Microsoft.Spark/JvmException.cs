using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Spark
{
    public class JvmException : Exception
    {
        private readonly string _jvmFullException;

        public JvmException(string jvmFullException)
        {
            _jvmFullException = jvmFullException;
        }

        public override string Message => _jvmFullException;
    }
}
