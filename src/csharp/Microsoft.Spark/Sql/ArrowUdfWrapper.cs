// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using static Microsoft.Spark.Sql.ArrowArrayHelpers;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal sealed class ArrowUdfWrapper<T, TResult> 
        where T : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T, TResult> _func;

        internal ArrowUdfWrapper(Func<T, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T)columns[argOffsets[0]]);
            }

            return CreateEmptyArray<TResult>();
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    [UdfWrapper]
    internal sealed class ArrowUdfWrapper<T1, T2, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]]);
            }

            return CreateEmptyArray<TResult>();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where T3 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, T3, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]],
                    (T3)columns[argOffsets[2]]);
            }

            return CreateEmptyArray<TResult>();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where T3 : IArrowArray
        where T4 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, T3, T4, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]],
                    (T3)columns[argOffsets[2]],
                    (T4)columns[argOffsets[3]]);
            }

            return CreateEmptyArray<TResult>();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where T3 : IArrowArray
        where T4 : IArrowArray
        where T5 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, T3, T4, T5, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]],
                    (T3)columns[argOffsets[2]],
                    (T4)columns[argOffsets[3]],
                    (T5)columns[argOffsets[4]]);
            }

            return CreateEmptyArray<TResult>();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where T3 : IArrowArray
        where T4 : IArrowArray
        where T5 : IArrowArray
        where T6 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]],
                    (T3)columns[argOffsets[2]],
                    (T4)columns[argOffsets[3]],
                    (T5)columns[argOffsets[4]],
                    (T6)columns[argOffsets[5]]);
            }

            return CreateEmptyArray<TResult>();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where T3 : IArrowArray
        where T4 : IArrowArray
        where T5 : IArrowArray
        where T6 : IArrowArray
        where T7 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]],
                    (T3)columns[argOffsets[2]],
                    (T4)columns[argOffsets[3]],
                    (T5)columns[argOffsets[4]],
                    (T6)columns[argOffsets[5]],
                    (T7)columns[argOffsets[6]]);
            }

            return CreateEmptyArray<TResult>();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where T3 : IArrowArray
        where T4 : IArrowArray
        where T5 : IArrowArray
        where T6 : IArrowArray
        where T7 : IArrowArray
        where T8 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]],
                    (T3)columns[argOffsets[2]],
                    (T4)columns[argOffsets[3]],
                    (T5)columns[argOffsets[4]],
                    (T6)columns[argOffsets[5]],
                    (T7)columns[argOffsets[6]],
                    (T8)columns[argOffsets[7]]);
            }

            return CreateEmptyArray<TResult>();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where T3 : IArrowArray
        where T4 : IArrowArray
        where T5 : IArrowArray
        where T6 : IArrowArray
        where T7 : IArrowArray
        where T8 : IArrowArray
        where T9 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]],
                    (T3)columns[argOffsets[2]],
                    (T4)columns[argOffsets[3]],
                    (T5)columns[argOffsets[4]],
                    (T6)columns[argOffsets[5]],
                    (T7)columns[argOffsets[6]],
                    (T8)columns[argOffsets[7]],
                    (T9)columns[argOffsets[8]]);
            }

            return CreateEmptyArray<TResult>();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> 
        where T1 : IArrowArray
        where T2 : IArrowArray
        where T3 : IArrowArray
        where T4 : IArrowArray
        where T5 : IArrowArray
        where T6 : IArrowArray
        where T7 : IArrowArray
        where T8 : IArrowArray
        where T9 : IArrowArray
        where T10 : IArrowArray
        where TResult : IArrowArray
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;

            if (length > 0)
            {
                return _func(
                    (T1)columns[argOffsets[0]],
                    (T2)columns[argOffsets[1]],
                    (T3)columns[argOffsets[2]],
                    (T4)columns[argOffsets[3]],
                    (T5)columns[argOffsets[4]],
                    (T6)columns[argOffsets[5]],
                    (T7)columns[argOffsets[6]],
                    (T8)columns[argOffsets[7]],
                    (T9)columns[argOffsets[8]],
                    (T10)columns[argOffsets[9]]);
            }

            return CreateEmptyArray<TResult>();
        }
    }
}
