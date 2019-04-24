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
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    internal sealed class ArrowUdfWrapper<TResult>
    {
        private readonly Func<TResult> _func;

        internal ArrowUdfWrapper(Func<TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            return ToArrowArray(new TResult[1] { _func() });
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    internal sealed class ArrowUdfWrapper<T, TResult>
    {
        private readonly Func<T, TResult> _func;

        internal ArrowUdfWrapper(Func<T, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T> arg0Getter = GetGetter<T>(columns[argOffsets[0]]);

                for (int i = 0; i < length; ++i)
                {
                    append(_func(arg0Getter(i)));
                }
            }
            return build();
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    internal sealed class ArrowUdfWrapper<T1, T2, TResult>
    {
        private readonly Func<T1, T2, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);

                for (int i = 0; i < length; ++i)
                {
                    append(_func(arg0Getter(i), arg1Getter(i)));
                }
            }

            return build();
        }
    }

    /// <summary>
    /// Wraps the given Func object, which represents a UDF.
    /// </summary>
    /// <typeparam name="T1">Specifies the type of the first argument to the UDF.</typeparam>
    /// <typeparam name="T2">Specifies the type of the second argument to the UDF.</typeparam>
    /// <typeparam name="T3">Specifies the type of the third argument to the UDF.</typeparam>
    /// <typeparam name="TResult">Specifies the return type of the UDF.</typeparam>
    internal sealed class ArrowUdfWrapper<T1, T2, T3, TResult>
    {
        private readonly Func<T1, T2, T3, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);
                Func<int, T3> arg2Getter = GetGetter<T3>(columns[argOffsets[2]]);

                for (int i = 0; i < length; ++i)
                {
                    append(_func(arg0Getter(i), arg1Getter(i), arg2Getter(i)));
                }
            }

            return build();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, TResult>
    {
        private readonly Func<T1, T2, T3, T4, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);
                Func<int, T3> arg2Getter = GetGetter<T3>(columns[argOffsets[2]]);
                Func<int, T4> arg3Getter = GetGetter<T4>(columns[argOffsets[3]]);

                for (int i = 0; i < length; ++i)
                {
                    append(_func(arg0Getter(i), arg1Getter(i), arg2Getter(i), arg3Getter(i)));
                }
            }

            return build();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(int splitIndex, ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);
                Func<int, T3> arg2Getter = GetGetter<T3>(columns[argOffsets[2]]);
                Func<int, T4> arg3Getter = GetGetter<T4>(columns[argOffsets[3]]);
                Func<int, T5> arg4Getter = GetGetter<T5>(columns[argOffsets[4]]);

                for (int i = 0; i < length; ++i)
                {
                    append(
                        _func(
                            arg0Getter(i),
                            arg1Getter(i),
                            arg2Getter(i),
                            arg3Getter(i),
                            arg4Getter(i)));
                }
            }

            return build();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);
                Func<int, T3> arg2Getter = GetGetter<T3>(columns[argOffsets[2]]);
                Func<int, T4> arg3Getter = GetGetter<T4>(columns[argOffsets[3]]);
                Func<int, T5> arg4Getter = GetGetter<T5>(columns[argOffsets[4]]);
                Func<int, T6> arg5Getter = GetGetter<T6>(columns[argOffsets[5]]);

                for (int i = 0; i < length; ++i)
                {
                    append(
                        _func(
                            arg0Getter(i),
                            arg1Getter(i),
                            arg2Getter(i),
                            arg3Getter(i),
                            arg4Getter(i),
                            arg5Getter(i)));
                }
            }

            return build();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);
                Func<int, T3> arg2Getter = GetGetter<T3>(columns[argOffsets[2]]);
                Func<int, T4> arg3Getter = GetGetter<T4>(columns[argOffsets[3]]);
                Func<int, T5> arg4Getter = GetGetter<T5>(columns[argOffsets[4]]);
                Func<int, T6> arg5Getter = GetGetter<T6>(columns[argOffsets[5]]);
                Func<int, T7> arg6Getter = GetGetter<T7>(columns[argOffsets[6]]);

                for (int i = 0; i < length; ++i)
                {
                    append(
                        _func(
                            arg0Getter(i),
                            arg1Getter(i),
                            arg2Getter(i),
                            arg3Getter(i),
                            arg4Getter(i),
                            arg5Getter(i),
                            arg6Getter(i)));
                }
            }

            return build();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(int splitIndex, ReadOnlyMemory<IArrowArray> input, int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);
                Func<int, T3> arg2Getter = GetGetter<T3>(columns[argOffsets[2]]);
                Func<int, T4> arg3Getter = GetGetter<T4>(columns[argOffsets[3]]);
                Func<int, T5> arg4Getter = GetGetter<T5>(columns[argOffsets[4]]);
                Func<int, T6> arg5Getter = GetGetter<T6>(columns[argOffsets[5]]);
                Func<int, T7> arg6Getter = GetGetter<T7>(columns[argOffsets[6]]);
                Func<int, T8> arg7Getter = GetGetter<T8>(columns[argOffsets[7]]);

                for (int i = 0; i < length; ++i)
                {
                    append(
                        _func(
                            arg0Getter(i),
                            arg1Getter(i),
                            arg2Getter(i),
                            arg3Getter(i),
                            arg4Getter(i),
                            arg5Getter(i),
                            arg6Getter(i),
                            arg7Getter(i)));
                }
            }

            return build();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);
                Func<int, T3> arg2Getter = GetGetter<T3>(columns[argOffsets[2]]);
                Func<int, T4> arg3Getter = GetGetter<T4>(columns[argOffsets[3]]);
                Func<int, T5> arg4Getter = GetGetter<T5>(columns[argOffsets[4]]);
                Func<int, T6> arg5Getter = GetGetter<T6>(columns[argOffsets[5]]);
                Func<int, T7> arg6Getter = GetGetter<T7>(columns[argOffsets[6]]);
                Func<int, T8> arg7Getter = GetGetter<T8>(columns[argOffsets[7]]);
                Func<int, T9> arg8Getter = GetGetter<T9>(columns[argOffsets[8]]);

                for (int i = 0; i < length; ++i)
                {
                    append(
                        _func(
                            arg0Getter(i),
                            arg1Getter(i),
                            arg2Getter(i),
                            arg3Getter(i),
                            arg4Getter(i),
                            arg5Getter(i),
                            arg6Getter(i),
                            arg7Getter(i),
                            arg8Getter(i)));
                }
            }

            return build();
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
    internal sealed class ArrowUdfWrapper<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult>
    {
        private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> _func;

        internal ArrowUdfWrapper(Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, TResult> func)
        {
            _func = func;
        }

        internal IArrowArray Execute(
            int splitIndex,
            ReadOnlyMemory<IArrowArray> input,
            int[] argOffsets)
        {
            ReadOnlySpan<IArrowArray> columns = input.Span;
            int length = columns[0]?.Length ?? 0;
            Action<TResult> append =
                CreateArrowArray<TResult>(length, out Func<IArrowArray> build);

            if (length > 0)
            {
                Func<int, T1> arg0Getter = GetGetter<T1>(columns[argOffsets[0]]);
                Func<int, T2> arg1Getter = GetGetter<T2>(columns[argOffsets[1]]);
                Func<int, T3> arg2Getter = GetGetter<T3>(columns[argOffsets[2]]);
                Func<int, T4> arg3Getter = GetGetter<T4>(columns[argOffsets[3]]);
                Func<int, T5> arg4Getter = GetGetter<T5>(columns[argOffsets[4]]);
                Func<int, T6> arg5Getter = GetGetter<T6>(columns[argOffsets[5]]);
                Func<int, T7> arg6Getter = GetGetter<T7>(columns[argOffsets[6]]);
                Func<int, T8> arg7Getter = GetGetter<T8>(columns[argOffsets[7]]);
                Func<int, T9> arg8Getter = GetGetter<T9>(columns[argOffsets[8]]);
                Func<int, T10> arg9Getter = GetGetter<T10>(columns[argOffsets[9]]);

                for (int i = 0; i < length; ++i)
                {
                    append(
                        _func(
                            arg0Getter(i),
                            arg1Getter(i),
                            arg2Getter(i),
                            arg3Getter(i),
                            arg4Getter(i),
                            arg5Getter(i),
                            arg6Getter(i),
                            arg7Getter(i),
                            arg8Getter(i),
                            arg9Getter(i)));
                }
            }

            return build();
        }
    }
}
