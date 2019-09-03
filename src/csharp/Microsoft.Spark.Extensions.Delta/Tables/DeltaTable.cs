using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Extensions.Delta.Tables
{
    public sealed class DeltaTable : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal DeltaTable(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="alias"></param>
        /// <returns></returns>
        public DeltaTable As(string alias) =>
            new DeltaTable((JvmObjectReference)_jvmObject.Invoke("as", alias));

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataFrame ToDF() => new DataFrame((JvmObjectReference)_jvmObject.Invoke("toDF"));

        /// <summary>
        /// 
        /// </summary>
        /// <param name="retentionHours"></param>
        /// <returns></returns>
        public DataFrame Vacuum(double retentionHours) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("vacuum", retentionHours));

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataFrame Vacuum() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("vacuum"));

        /// <summary>
        /// 
        /// </summary>
        /// <param name="limit"></param>
        /// <returns></returns>
        public DataFrame History(int limit) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("history", limit));

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataFrame History() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("history"));

        /// <summary>
        /// 
        /// </summary>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataFrame Delete(string condition) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("delete", condition));

        /// <summary>
        /// 
        /// </summary>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DataFrame Delete(Column condition) =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke(
                "delete", ((IJvmObjectReferenceProvider)condition).Reference));

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DataFrame Delete() =>
            new DataFrame((JvmObjectReference)_jvmObject.Invoke("delete"));

        /// <summary>
        /// 
        /// </summary>
        /// <param name="set"></param>
        public void Update(Dictionary<string, Column> set) =>
            throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="set"></param>
        public void Update(Column condition, Dictionary<string, Column> set) =>
            throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="set"></param>
        public void UpdateExpr(Dictionary<string, string> set) =>
            throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="condition"></param>
        /// <param name="set"></param>
        public void UpdateExpr(string condition, Dictionary<string, string> set) =>
            throw new NotImplementedException();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DeltaMergeBuilder Merge(DataFrame source, string condition) =>
            new DeltaMergeBuilder((JvmObjectReference)_jvmObject.Invoke(
                "merge", ((IJvmObjectReferenceProvider)source).Reference, condition));

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public DeltaMergeBuilder Merge(DataFrame source, Column condition) =>
            new DeltaMergeBuilder((JvmObjectReference)_jvmObject.Invoke(
                "merge",
                ((IJvmObjectReferenceProvider)source).Reference,
                ((IJvmObjectReferenceProvider)condition).Reference));

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        public static DeltaTable ForPath(string path) =>
            new DeltaTable((JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "io.delta.tables.DeltaTable",
                "forPath",
                path));

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sparkSession"></param>
        /// <param name="path"></param>
        /// <returns></returns>
        public static DeltaTable ForPath(SparkSession sparkSession, string path) =>
            new DeltaTable((JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                "io.delta.tables.DeltaTable",
                "forPath",
                ((IJvmObjectReferenceProvider)sparkSession).Reference,
                path));
    }
}
