// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Types;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql.Expressions
{
    /// <summary>
    /// UserDefinedFunction is not exposed to the user directly.
    /// Use Functions.Udf to create this object indirectly.
    /// </summary>
    internal sealed class UserDefinedFunction : IJvmObjectReferenceProvider
    {
        internal static UserDefinedFunction Create(
            string name,
            byte[] command,
            UdfUtils.PythonEvalType evalType,
            string returnType)
        {
            return Create(SparkEnvironment.JvmBridge, name, command, evalType, returnType);
        }

        internal static UserDefinedFunction Create(
            IJvmBridge jvm,
            string name,
            byte[] command,
            UdfUtils.PythonEvalType evalType,
            string returnType)
        {
            return new UserDefinedFunction(
                jvm.CallConstructor(
                    "org.apache.spark.sql.execution.python.UserDefinedPythonFunction",
                    name,
                    UdfUtils.CreatePythonFunction(jvm, command),
                    DataType.FromJson(jvm, returnType),
                    (int)evalType,
                    true // udfDeterministic
                    ));
        }

        internal UserDefinedFunction(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

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

        internal Column Apply(params Column[] columns)
        {
            return new Column((JvmObjectReference)Reference.Invoke("apply", (object)columns));
        }
    }
}
