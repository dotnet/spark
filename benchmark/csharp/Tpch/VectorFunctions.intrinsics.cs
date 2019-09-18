// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Apache.Arrow;

namespace Tpch
{
    internal static class VectorFunctionsIntrinsics
    {
        internal static unsafe DoubleArray ComputeTotal(DoubleArray price, DoubleArray discount, DoubleArray tax)
        {
            if ((price.Length != discount.Length) || (price.Length != tax.Length))
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            int length = price.Length;
            int vectorizationLength = length - (length % 4);

            DoubleArray.Builder builder = new DoubleArray.Builder().Reserve(length);
            Span<double> buffer = stackalloc double[4];

            fixed (double* pExtendedPrice = price.Values)
            fixed (double* pDiscount = discount.Values)
            fixed (double* pTaxes = discount.Values)
            fixed (double* pBuffer = buffer)
            {
                Vector256<double> ones = Vector256.Create(1.0);

                int i = 0;
                for (; i < vectorizationLength; i += 4)
                {
                    Vector256<double> r = Avx.Multiply(
                        Avx.Multiply(
                            Avx.LoadVector256(pExtendedPrice + i),
                            Avx.Subtract(ones, Avx.LoadVector256(pDiscount + i))),
                        Avx.Add(ones, Avx.LoadVector256(pTaxes + i)));

                    Avx.Store(pBuffer, r);
                    builder.Append(buffer);
                }

                for (; i < length; i += 1)
                {
                    builder.Append(pExtendedPrice[i] * (1 - pDiscount[i]) * (1 + pTaxes[i]));
                }
            }

            return builder.Build();
        }

        internal static unsafe DoubleArray ComputeDiscountPrice(DoubleArray price, DoubleArray discount)
        {
            if (price.Length != discount.Length)
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            int length = price.Length;
            int vectorizationLength = length - (length % 4);

            DoubleArray.Builder builder = new DoubleArray.Builder().Reserve(length);
            Span<double> buffer = stackalloc double[4];

            fixed (double* pExtendedPrice = price.Values)
            fixed (double* pDiscount = discount.Values)
            fixed (double* pBuffer = buffer)
            {
                Vector256<double> ones = Vector256.Create(1.0);
                
                int i = 0;
                for (; i < vectorizationLength; i += 4)
                {
                    Vector256<double> r = Avx.Multiply(
                        Avx.LoadVector256(pExtendedPrice + i),
                        Avx.Subtract(ones, Avx.LoadVector256(pDiscount + i)));

                    Avx.Store(pBuffer, r);
                    builder.Append(buffer);
                }

                for (; i < length; i += 1)
                {
                    builder.Append(pExtendedPrice[i] * (1 - pDiscount[i]));
                }
            }

            return builder.Build();
        }
    }
}
