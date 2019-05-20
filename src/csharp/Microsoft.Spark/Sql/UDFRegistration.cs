// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Sql.Expressions;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Functions for registering user-defined functions.
    /// </summary>
    public sealed class UdfRegistration : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal UdfRegistration(JvmObjectReference jvmObject)
        {
            _jvmObject = jvmObject;
        }

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<TResult>(string name, Func<TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T, TResult>(string name, Func<T, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, TResult>(string name, Func<T1, T2, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, T3, TResult>(string name, Func<T1, T2, T3, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, T3, T4, TResult>(string name, Func<T1, T2, T3, T4, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, T3, T4, T5, TResult>(
            string name,
            Func<T1, T2, T3, T4, T5, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, T3, T4, T5, T6, TResult>(
            string name,
            Func<T1, T2, T3, T4, T5, T6, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, T3, T4, T5, T6, T7, TResult>(
            string name,
            Func<T1, T2, T3, T4, T5, T6, T7, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, T3, T4, T5, T6, T7, T8, TResult>(
            string name,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="T9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>(
            string name,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Registers the given delegate as a user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
        /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
        /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
        /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
        /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
        /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
        /// <typeparam name="T8">Specifies the type of the eighth argument to the UDF.</typeparam>
        /// <typeparam name="T9">Specifies the type of the ninth argument to the UDF.</typeparam>
        /// <typeparam name="T10">Specifies the type of the tenth argument to the UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
        /// <param name="name">The UDF name.</param>
        /// <param name="f">The UDF function implementation.</param>
        public void Register<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>(
            string name,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> f)
        {
            Register<TResult>(name, UdfUtils.CreateUdfWrapper(f));
        }

        /// <summary>
        /// Register a Java UDF class using reflection.
        /// </summary>
        /// <typeparam name="TResult">Return type</typeparam>
        /// <param name="name">Name of the UDF</param>
        /// <param name="className">Class name that defines UDF</param>
        public void RegisterJava<TResult>(string name, string className)
        {
            _jvmObject.Invoke("registerJava", name, className, GetDataType<TResult>());
        }

        /// <summary>
        /// Register a Java UDAF class using reflection.
        /// </summary>
        /// <param name="name">Name of the UDAF</param>
        /// <param name="className">Class name that defines UDAF</param>
        public void RegisterJavaUDAF(string name, string className)
        {
            _jvmObject.Invoke("registerJavaUDAF", name, className);
        }

        /// <summary>
        /// Helper function to register wrapped udf.
        /// </summary>
        /// <typeparam name="TResult">Return type of the udf</typeparam>
        /// <param name="name">Name of the udf</param>
        /// <param name="func">Wrapped UDF function</param>
        private void Register<TResult>(string name, Delegate func)
        {
            byte[] command = CommandSerDe.Serialize(
                func,
                CommandSerDe.SerializedMode.Row,
                CommandSerDe.SerializedMode.Row);

            JvmObjectReference pythonFunction =
                UdfUtils.CreatePythonFunction(_jvmObject.Jvm, command);

            var udf = new UserDefinedFunction(
                _jvmObject.Jvm.CallConstructor(
                    "org.apache.spark.sql.execution.python.UserDefinedPythonFunction",
                    name,
                    pythonFunction,
                    GetDataType<TResult>(),
                    (int)UdfUtils.GetPythonEvalType(),
                    true // udfDeterministic
                    ));

            _jvmObject.Invoke("registerPython", name, udf);
        }

        private JvmObjectReference GetDataType<T>()
        {
            return (JvmObjectReference)_jvmObject.Jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.types.DataType",
                "fromJson",
                $"{UdfUtils.GetReturnType(typeof(T))}");
        }
    }
}
