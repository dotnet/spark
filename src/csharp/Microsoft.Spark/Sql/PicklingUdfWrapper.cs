// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Diagnostics;
using System.Reflection;

namespace Microsoft.Spark.Sql
{
    internal static class PicklingUdfWrapperHelper
    {
        internal static T Convert<T>(object obj)
        {
            if (obj == null)
            {
                return default;
            }

            Type type = obj.GetType();
            return (T)(type switch
            {
                _ when type == typeof(ArrayList) => ConvertToArray<T>((ArrayList)obj),
                _ => obj
            });
        }

        private static object ConvertToArray<T>(ArrayList arrayList)
        {
            Type genericType = typeof(T);
            Debug.Assert(genericType.IsArray);
            Type genericElementType = genericType.GetElementType();

            int length = arrayList.Count;
            Array convertedArray = Array.CreateInstance(genericElementType, length);
            MethodInfo convertMethod = typeof(PicklingUdfWrapperHelper)
                .GetMethod("Convert", BindingFlags.Static | BindingFlags.NonPublic)
                .MakeGenericMethod(new[] { genericElementType });

            for (int i = 0; i < length; ++i)
            {

                object convertedElement = convertMethod.Invoke(null, new[] { arrayList[i] });
                Debug.Assert(genericElementType == convertedElement.GetType());
                convertedArray.SetValue(convertedElement, i);
            }

            return convertedArray;
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal class PicklingUdfWrapper<TResult>
    {
        private readonly Func<TResult> _func;

        internal PicklingUdfWrapper(Func<TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func();
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal class PicklingUdfWrapper<T, TResult>
    {
        private readonly Func<T, TResult> _func;

        internal PicklingUdfWrapper(Func<T, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(PicklingUdfWrapperHelper.Convert<T>(input[argOffsets[0]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, TResult>
    {
        private readonly Func<T1, T2, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, T3, TResult>
    {
        private readonly Func<T1, T2, T3, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]),
                PicklingUdfWrapperHelper.Convert<T3>(input[argOffsets[2]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
    /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, T3, T4, TResult>
    {
        private readonly Func<T1, T2, T3, T4, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]),
                PicklingUdfWrapperHelper.Convert<T3>(input[argOffsets[2]]),
                PicklingUdfWrapperHelper.Convert<T4>(input[argOffsets[3]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
    /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
    /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]),
                PicklingUdfWrapperHelper.Convert<T3>(input[argOffsets[2]]),
                PicklingUdfWrapperHelper.Convert<T4>(input[argOffsets[3]]),
                PicklingUdfWrapperHelper.Convert<T5>(input[argOffsets[4]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
    /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
    /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
    /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]),
                PicklingUdfWrapperHelper.Convert<T3>(input[argOffsets[2]]),
                PicklingUdfWrapperHelper.Convert<T4>(input[argOffsets[3]]),
                PicklingUdfWrapperHelper.Convert<T5>(input[argOffsets[4]]),
                PicklingUdfWrapperHelper.Convert<T6>(input[argOffsets[5]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
    /// <typeparam name="T4">Specifies the type of the fourth argument to the UDF.</typeparam>
    /// <typeparam name="T5">Specifies the type of the fifth argument to the UDF.</typeparam>
    /// <typeparam name="T6">Specifies the type of the sixth argument to the UDF.</typeparam>
    /// <typeparam name="T7">Specifies the type of the seventh argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]),
                PicklingUdfWrapperHelper.Convert<T3>(input[argOffsets[2]]),
                PicklingUdfWrapperHelper.Convert<T4>(input[argOffsets[3]]),
                PicklingUdfWrapperHelper.Convert<T5>(input[argOffsets[4]]),
                PicklingUdfWrapperHelper.Convert<T6>(input[argOffsets[5]]),
                PicklingUdfWrapperHelper.Convert<T7>(input[argOffsets[6]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
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
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]),
                PicklingUdfWrapperHelper.Convert<T3>(input[argOffsets[2]]),
                PicklingUdfWrapperHelper.Convert<T4>(input[argOffsets[3]]),
                PicklingUdfWrapperHelper.Convert<T5>(input[argOffsets[4]]),
                PicklingUdfWrapperHelper.Convert<T6>(input[argOffsets[5]]),
                PicklingUdfWrapperHelper.Convert<T7>(input[argOffsets[6]]),
                PicklingUdfWrapperHelper.Convert<T8>(input[argOffsets[7]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
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
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> func)
        {
            _func = func;
        }
        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]),
                PicklingUdfWrapperHelper.Convert<T3>(input[argOffsets[2]]),
                PicklingUdfWrapperHelper.Convert<T4>(input[argOffsets[3]]),
                PicklingUdfWrapperHelper.Convert<T5>(input[argOffsets[4]]),
                PicklingUdfWrapperHelper.Convert<T6>(input[argOffsets[5]]),
                PicklingUdfWrapperHelper.Convert<T7>(input[argOffsets[6]]),
                PicklingUdfWrapperHelper.Convert<T8>(input[argOffsets[7]]),
                PicklingUdfWrapperHelper.Convert<T9>(input[argOffsets[8]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
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
    [UdfWrapper]
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                PicklingUdfWrapperHelper.Convert<T1>(input[argOffsets[0]]),
                PicklingUdfWrapperHelper.Convert<T2>(input[argOffsets[1]]),
                PicklingUdfWrapperHelper.Convert<T3>(input[argOffsets[2]]),
                PicklingUdfWrapperHelper.Convert<T4>(input[argOffsets[3]]),
                PicklingUdfWrapperHelper.Convert<T5>(input[argOffsets[4]]),
                PicklingUdfWrapperHelper.Convert<T6>(input[argOffsets[5]]),
                PicklingUdfWrapperHelper.Convert<T7>(input[argOffsets[6]]),
                PicklingUdfWrapperHelper.Convert<T8>(input[argOffsets[7]]),
                PicklingUdfWrapperHelper.Convert<T9>(input[argOffsets[8]]),
                PicklingUdfWrapperHelper.Convert<T10>(input[argOffsets[9]]));
        }
    }
}
