using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Extensions.Delta.Tables
{
    public class DeltaMergeNotMatchedActionBuilder : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal DeltaMergeNotMatchedActionBuilder(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public DeltaMergeBuilder Insert(Dictionary<string, Column> values) =>
            throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public DeltaMergeBuilder InsertExpr(Dictionary<string, string> values) =>
            throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DeltaMergeBuilder InsertAll() =>
            new DeltaMergeBuilder((JvmObjectReference)_jvmObject.Invoke("insertAll"));
    }
}
