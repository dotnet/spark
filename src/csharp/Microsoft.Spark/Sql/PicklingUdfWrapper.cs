// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    internal class PicklingUdfWrapper<TResult> : IUdfWrapper
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
    internal class PicklingUdfWrapper<T, TResult> : IUdfWrapper
    {
        private readonly Func<T, TResult> _func;

        internal PicklingUdfWrapper(Func<T, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func((T)(input[argOffsets[0]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    internal class PicklingUdfWrapper<T1, T2, TResult> : IUdfWrapper
    {
        private readonly Func<T1, T2, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func((T1)(input[argOffsets[0]]), (T2)(input[argOffsets[1]]));
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    internal class PicklingUdfWrapper<T1, T2, T3, TResult> : IUdfWrapper
    {
        private readonly Func<T1, T2, T3, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                (T1)(input[argOffsets[0]]),
                (T2)(input[argOffsets[1]]),
                (T3)(input[argOffsets[2]]));
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
    internal class PicklingUdfWrapper<T1, T2, T3, T4, TResult> : IUdfWrapper
    {
        private readonly Func<T1, T2, T3, T4, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                (T1)(input[argOffsets[0]]),
                (T2)(input[argOffsets[1]]),
                (T3)(input[argOffsets[2]]),
                (T4)(input[argOffsets[3]]));
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
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, TResult> : IUdfWrapper
    {
        private readonly Func<T1, T2, T3, T4, T5, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                (T1)(input[argOffsets[0]]),
                (T2)(input[argOffsets[1]]),
                (T3)(input[argOffsets[2]]),
                (T4)(input[argOffsets[3]]),
                (T5)(input[argOffsets[4]]));
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
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, TResult> : IUdfWrapper
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                (T1)(input[argOffsets[0]]),
                (T2)(input[argOffsets[1]]),
                (T3)(input[argOffsets[2]]),
                (T4)(input[argOffsets[3]]),
                (T5)(input[argOffsets[4]]),
                (T6)(input[argOffsets[5]]));
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
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult> : IUdfWrapper
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                (T1)(input[argOffsets[0]]),
                (T2)(input[argOffsets[1]]),
                (T3)(input[argOffsets[2]]),
                (T4)(input[argOffsets[3]]),
                (T5)(input[argOffsets[4]]),
                (T6)(input[argOffsets[5]]),
                (T7)(input[argOffsets[6]]));
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
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult> : IUdfWrapper
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                (T1)(input[argOffsets[0]]),
                (T2)(input[argOffsets[1]]),
                (T3)(input[argOffsets[2]]),
                (T4)(input[argOffsets[3]]),
                (T5)(input[argOffsets[4]]),
                (T6)(input[argOffsets[5]]),
                (T7)(input[argOffsets[6]]),
                (T8)(input[argOffsets[7]]));
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
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> : IUdfWrapper
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> func)
        {
            _func = func;
        }
        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                (T1)(input[argOffsets[0]]),
                (T2)(input[argOffsets[1]]),
                (T3)(input[argOffsets[2]]),
                (T4)(input[argOffsets[3]]),
                (T5)(input[argOffsets[4]]),
                (T6)(input[argOffsets[5]]),
                (T7)(input[argOffsets[6]]),
                (T8)(input[argOffsets[7]]),
                (T9)(input[argOffsets[8]]));
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
    internal class PicklingUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>
        : IUdfWrapper
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            return _func(
                (T1)(input[argOffsets[0]]),
                (T2)(input[argOffsets[1]]),
                (T3)(input[argOffsets[2]]),
                (T4)(input[argOffsets[3]]),
                (T5)(input[argOffsets[4]]),
                (T6)(input[argOffsets[5]]),
                (T7)(input[argOffsets[6]]),
                (T8)(input[argOffsets[7]]),
                (T9)(input[argOffsets[8]]),
                (T10)(input[argOffsets[9]]));
        }
    }
}
