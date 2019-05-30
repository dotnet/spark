// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql.Expressions
{
    /// <summary>
    /// UserDefinedFunction is not exposed to the user directly.
    /// Use Functions.Udf to create this object indirectly.
    /// </summary>
    internal sealed class UserDefinedFunction : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal static UserDefinedFunction Create(
            string name,
            byte[] command,
            UdfUtils.PythonEvalType evalType,
            string returnType)
        {
            IJvmBridge jvm = SparkEnvironment.JvmBridge;

            JvmObjectReference hashTableReference = jvm.CallConstructor("java.util.Hashtable");
            JvmObjectReference arrayListReference = jvm.CallConstructor("java.util.ArrayList");

            var dataType = (JvmObjectReference)jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.types.DataType",
                "fromJson",
                $"{returnType}");

            var pythonFunction = (JvmObjectReference)jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.api.dotnet.SQLUtils",
                "createPythonFunction",
                command,
                hashTableReference, // Environment variables
                arrayListReference, // Python includes
                SparkEnvironment.ConfigurationService.GetWorkerExePath(),
                "1.0",
                arrayListReference, // Broadcast variables
                null); // Accumulator

            return new UserDefinedFunction(
                jvm.CallConstructor(
                    "org.apache.spark.sql.execution.python.UserDefinedPythonFunction",
                    name,
                    pythonFunction,
                    dataType,
                    (int)evalType,
                    true // udfDeterministic
                    ));
        }

        internal UserDefinedFunction(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        // Apply0 and Apply10 is defined so that a corresponding Func<> can
        // be constructed in Functions.Udf.

        internal Column Apply0()
        {
            return Apply();
        }

        internal Column Apply1(Column col)
        {
            return Apply(col);
        }

        internal Column Apply2(Column col1, Column col2)
        {
            return Apply(col1, col2);
        }

        internal Column Apply3(Column col1, Column col2, Column col3)
        {
            return Apply(col1, col2, col3);
        }

        internal Column Apply4(Column col1, Column col2, Column col3, Column col4)
        {
            return Apply(col1, col2, col3, col4);
        }

        internal Column Apply5(Column col1, Column col2, Column col3, Column col4, Column col5)
        {
            return Apply(col1, col2, col3, col4, col5);
        }

        internal Column Apply6(
            Column col1,
            Column col2,
            Column col3,
            Column col4,
            Column col5,
            Column col6)
        {
            return Apply(col1, col2, col3, col4, col5, col6);
        }

        internal Column Apply7(
            Column col1,
            Column col2,
            Column col3,
            Column col4,
            Column col5,
            Column col6,
            Column col7)
        {
            return Apply(col1, col2, col3, col4, col5, col6, col7);
        }

        internal Column Apply8(
            Column col1,
            Column col2,
            Column col3,
            Column col4,
            Column col5,
            Column col6,
            Column col7,
            Column col8)
        {
            return Apply(col1, col2, col3, col4, col5, col6, col7, col8);
        }

        internal Column Apply9(
            Column col1,
            Column col2,
            Column col3,
            Column col4,
            Column col5,
            Column col6,
            Column col7,
            Column col8,
            Column col9)
        {
            return Apply(col1, col2, col3, col4, col5, col6, col7, col8, col9);
        }

        internal Column Apply10(
            Column col1,
            Column col2,
            Column col3,
            Column col4,
            Column col5,
            Column col6,
            Column col7,
            Column col8,
            Column col9,
            Column col10)
        {
            return Apply(col1, col2, col3, col4, col5, col6, col7, col8, col9, col10);
        }

        private Column Apply(params Column[] columns)
        {
            return new Column((JvmObjectReference)_jvmObject.Invoke("apply", (object)columns));
        }
    }
}
