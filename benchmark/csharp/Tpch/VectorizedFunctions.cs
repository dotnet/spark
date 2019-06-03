// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using Apache.Arrow;

namespace Tpch
{
    internal static class VectorizedFunctions
    {
        internal unsafe static DoubleArray ComputeTotal(DoubleArray price, DoubleArray discount, DoubleArray tax)
        {
            if (price.Length != discount.Length || price.Length != tax.Length)
                throw new ArgumentException("Arrays need to be the same length");

            int length = price.Length;
            var builder = new ArrowBuffer.Builder<double>(length);
            ReadOnlySpan<double> prices = price.Values;
            ReadOnlySpan<double> discounts = discount.Values;
            ReadOnlySpan<double> taxes = tax.Values;
            for (int i = 0; i < length; i++)
            {
                builder.Append(prices[i] * (1 - discounts[i]) * (1 + taxes[i]));
            }
            //Vector<double> ones = new Vector<double>(1d);

            //fixed (double* pStartExtendedPrice = price.Values)
            //fixed (double* pStartDiscount = discount.Values)
            //fixed (double* pStartTax = tax.Values)
            //{
            //    double* pExtendedPrice = pStartExtendedPrice;
            //    double* pDiscount = pStartDiscount;
            //    double* pTax = pStartTax;
            //    double* pExtendedPriceEnd = pExtendedPrice + length;

            //    double* pVectorizationEnd = pExtendedPriceEnd - 4;

            //    while (pExtendedPrice <= pVectorizationEnd)
            //    {
            //        Vector<double> exP = Unsafe.ReadUnaligned<Vector<double>>(pExtendedPrice);
            //        Vector<double> dis = Unsafe.ReadUnaligned<Vector<double>>(pDiscount);
            //        Vector<double> t = Unsafe.ReadUnaligned<Vector<double>>(pTax);

            //        Vector<double> r = exP * ((ones - dis) * (ones + t));

            //        for (int i = 0; i < Vector<double>.Count; i++)
            //        {
            //            builder.Append(r[i]);
            //        }

            //        pExtendedPrice += 4;
            //        pDiscount += 4;
            //        pTax += 4;
            //    }

            //    while (pExtendedPrice <= pExtendedPriceEnd)
            //    {
            //        builder.Append(*pExtendedPrice * (1 - *pDiscount) * (1 + *pTax));

            //        pExtendedPrice++;
            //        pDiscount++;
            //        pTax++;
            //    }
            //}

            return new DoubleArray(
                builder.Build(),
                nullBitmapBuffer: ArrowBuffer.Empty,
                length: length,
                nullCount: 0,
                offset: 0);
        }

        internal unsafe static DoubleArray ComputeDiscountPrice(DoubleArray price, DoubleArray discount)
        {
            if (price.Length != discount.Length)
                throw new ArgumentException("Arrays need to be the same length");

            int length = price.Length;
            var builder = new ArrowBuffer.Builder<double>(length);
            ReadOnlySpan<double> prices = price.Values;
            ReadOnlySpan<double> discounts = discount.Values;
            for (int i = 0; i < length; i++)
            {
                builder.Append(prices[i] * (1 - discounts[i]));
            }

            //Vector<double> ones = new Vector<double>(1d);

            //fixed (double* pStartExtendedPrice = price.Values)
            //fixed (double* pStartDiscount = discount.Values)
            //{
            //    double* pExtendedPrice = pStartExtendedPrice;
            //    double* pDiscount = pStartDiscount;
            //    double* pExtendedPriceEnd = pExtendedPrice + length;

            //    double* pVectorizationEnd = pExtendedPriceEnd - 4;

            //    while (pExtendedPrice <= pVectorizationEnd)
            //    {
            //        Vector<double> exP = Unsafe.ReadUnaligned<Vector<double>>(pExtendedPrice);
            //        Vector<double> dis = Unsafe.ReadUnaligned<Vector<double>>(pDiscount);

            //        Vector<double> r = exP * (ones - dis);

            //        for (int i = 0; i < Vector<double>.Count; i++)
            //        {
            //            builder.Append(r[i]);
            //        }

            //        pExtendedPrice += 4;
            //        pDiscount += 4;
            //    }

            //    while (pExtendedPrice <= pExtendedPriceEnd)
            //    {
            //        builder.Append(*pExtendedPrice * (1 - *pDiscount));

            //        pExtendedPrice++;
            //        pDiscount++;
            //    }
            //}

            return new DoubleArray(
                builder.Build(),
                nullBitmapBuffer: ArrowBuffer.Empty,
                length: length,
                nullCount: 0,
                offset: 0);
        }
    }
}
