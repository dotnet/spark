// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;

namespace Tpch
{
    internal static class VectorFunctions
    {
        internal static DoubleArray ComputeTotal(DoubleArray price, DoubleArray discount, DoubleArray tax)
        {
            if ((price.Length != discount.Length) || (price.Length != tax.Length))
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            int length = price.Length;
            var builder = new DoubleArray.Builder().Reserve(length);
            ReadOnlySpan<double> prices = price.Values;
            ReadOnlySpan<double> discounts = discount.Values;
            ReadOnlySpan<double> taxes = tax.Values;
            for (int i = 0; i < length; ++i)
            {
                builder.Append(prices[i] * (1 - discounts[i]) * (1 + taxes[i]));
            }

            return builder.Build();
        }

        internal static DoubleArray ComputeDiscountPrice(DoubleArray price, DoubleArray discount)
        {
            if (price.Length != discount.Length)
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            int length = price.Length;
            var builder = new DoubleArray.Builder().Reserve(length);
            ReadOnlySpan<double> prices = price.Values;
            ReadOnlySpan<double> discounts = discount.Values;
            for (int i = 0; i < length; ++i)
            {
                builder.Append(prices[i] * (1 - discounts[i]));
            }

            return builder.Build();
        }
    }
}
