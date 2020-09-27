// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using static Microsoft.Spark.Utils.TypeConverter;

namespace Microsoft.Spark.Sql
{
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
        [NonSerialized]
        private bool? _sameT = null;

        private readonly Func<T, TResult> _func;

        internal PicklingUdfWrapper(Func<T, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param = input[argOffsets[0]];
            return _func((_sameT ??= param is T) ? (T)param : ConvertTo<T>(param));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[2];

        private readonly Func<T1, T2, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[3];

        private readonly Func<T1, T2, T3, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            object param3 = input[argOffsets[2]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2),
                (_sameT[2] ??= param3 is T3) ? (T3)param3 : ConvertTo<T3>(param3));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[4];

        private readonly Func<T1, T2, T3, T4, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            object param3 = input[argOffsets[2]];
            object param4 = input[argOffsets[3]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2),
                (_sameT[2] ??= param3 is T3) ? (T3)param3 : ConvertTo<T3>(param3),
                (_sameT[3] ??= param4 is T4) ? (T4)param4 : ConvertTo<T4>(param4));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[5];

        private readonly Func<T1, T2, T3, T4, T5, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            object param3 = input[argOffsets[2]];
            object param4 = input[argOffsets[3]];
            object param5 = input[argOffsets[4]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2),
                (_sameT[2] ??= param3 is T3) ? (T3)param3 : ConvertTo<T3>(param3),
                (_sameT[3] ??= param4 is T4) ? (T4)param4 : ConvertTo<T4>(param4),
                (_sameT[4] ??= param5 is T5) ? (T5)param5 : ConvertTo<T5>(param5));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[6];

        private readonly Func<T1, T2, T3, T4, T5, T6, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            object param3 = input[argOffsets[2]];
            object param4 = input[argOffsets[3]];
            object param5 = input[argOffsets[4]];
            object param6 = input[argOffsets[5]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2),
                (_sameT[2] ??= param3 is T3) ? (T3)param3 : ConvertTo<T3>(param3),
                (_sameT[3] ??= param4 is T4) ? (T4)param4 : ConvertTo<T4>(param4),
                (_sameT[4] ??= param5 is T5) ? (T5)param5 : ConvertTo<T5>(param5),
                (_sameT[5] ??= param6 is T6) ? (T6)param6 : ConvertTo<T6>(param6));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[7];

        private readonly Func<T1, T2, T3, T4, T5, T6, T7, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            object param3 = input[argOffsets[2]];
            object param4 = input[argOffsets[3]];
            object param5 = input[argOffsets[4]];
            object param6 = input[argOffsets[5]];
            object param7 = input[argOffsets[6]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2),
                (_sameT[2] ??= param3 is T3) ? (T3)param3 : ConvertTo<T3>(param3),
                (_sameT[3] ??= param4 is T4) ? (T4)param4 : ConvertTo<T4>(param4),
                (_sameT[4] ??= param5 is T5) ? (T5)param5 : ConvertTo<T5>(param5),
                (_sameT[5] ??= param6 is T6) ? (T6)param6 : ConvertTo<T6>(param6),
                (_sameT[6] ??= param7 is T7) ? (T7)param7 : ConvertTo<T7>(param7));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[8];

        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            object param3 = input[argOffsets[2]];
            object param4 = input[argOffsets[3]];
            object param5 = input[argOffsets[4]];
            object param6 = input[argOffsets[5]];
            object param7 = input[argOffsets[6]];
            object param8 = input[argOffsets[7]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2),
                (_sameT[2] ??= param3 is T3) ? (T3)param3 : ConvertTo<T3>(param3),
                (_sameT[3] ??= param4 is T4) ? (T4)param4 : ConvertTo<T4>(param4),
                (_sameT[4] ??= param5 is T5) ? (T5)param5 : ConvertTo<T5>(param5),
                (_sameT[5] ??= param6 is T6) ? (T6)param6 : ConvertTo<T6>(param6),
                (_sameT[6] ??= param7 is T7) ? (T7)param7 : ConvertTo<T7>(param7),
                (_sameT[7] ??= param8 is T8) ? (T8)param8 : ConvertTo<T8>(param8));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[9];

        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> func)
        {
            _func = func;
        }
        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            object param3 = input[argOffsets[2]];
            object param4 = input[argOffsets[3]];
            object param5 = input[argOffsets[4]];
            object param6 = input[argOffsets[5]];
            object param7 = input[argOffsets[6]];
            object param8 = input[argOffsets[7]];
            object param9 = input[argOffsets[8]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2),
                (_sameT[2] ??= param3 is T3) ? (T3)param3 : ConvertTo<T3>(param3),
                (_sameT[3] ??= param4 is T4) ? (T4)param4 : ConvertTo<T4>(param4),
                (_sameT[4] ??= param5 is T5) ? (T5)param5 : ConvertTo<T5>(param5),
                (_sameT[5] ??= param6 is T6) ? (T6)param6 : ConvertTo<T6>(param6),
                (_sameT[6] ??= param7 is T7) ? (T7)param7 : ConvertTo<T7>(param7),
                (_sameT[7] ??= param8 is T8) ? (T8)param8 : ConvertTo<T8>(param8),
                (_sameT[8] ??= param9 is T9) ? (T9)param9 : ConvertTo<T9>(param9));
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
        [NonSerialized]
        private readonly bool?[] _sameT = new bool?[10];

        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> _func;

        internal PicklingUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> func)
        {
            _func = func;
        }

        internal object Execute(int splitIndex, object[] input, int[] argOffsets)
        {
            object param1 = input[argOffsets[0]];
            object param2 = input[argOffsets[1]];
            object param3 = input[argOffsets[2]];
            object param4 = input[argOffsets[3]];
            object param5 = input[argOffsets[4]];
            object param6 = input[argOffsets[5]];
            object param7 = input[argOffsets[6]];
            object param8 = input[argOffsets[7]];
            object param9 = input[argOffsets[8]];
            object param10 = input[argOffsets[9]];
            return _func(
                (_sameT[0] ??= param1 is T1) ? (T1)param1 : ConvertTo<T1>(param1),
                (_sameT[1] ??= param2 is T2) ? (T2)param2 : ConvertTo<T2>(param2),
                (_sameT[2] ??= param3 is T3) ? (T3)param3 : ConvertTo<T3>(param3),
                (_sameT[3] ??= param4 is T4) ? (T4)param4 : ConvertTo<T4>(param4),
                (_sameT[4] ??= param5 is T5) ? (T5)param5 : ConvertTo<T5>(param5),
                (_sameT[5] ??= param6 is T6) ? (T6)param6 : ConvertTo<T6>(param6),
                (_sameT[6] ??= param7 is T7) ? (T7)param7 : ConvertTo<T7>(param7),
                (_sameT[7] ??= param8 is T8) ? (T8)param8 : ConvertTo<T8>(param8),
                (_sameT[8] ??= param9 is T9) ? (T9)param9 : ConvertTo<T9>(param9),
                (_sameT[9] ??= param10 is T10) ? (T10)param10 : ConvertTo<T10>(param10));
        }
    }
}
