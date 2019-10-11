// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Data;

namespace Tpch
{
    internal static class VectorFunctions
    {
        internal static PrimitiveColumn<double> ComputeTotal(PrimitiveColumn<double> price, PrimitiveColumn<double> discount, PrimitiveColumn<double> tax)
        {
            if ((price.Length != discount.Length) || (price.Length != tax.Length))
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            PrimitiveColumn<double> ret = new PrimitiveColumn<double>("Prices", price.Length);

            IEnumerable<ReadOnlyMemory<double>> readOnlyTaxes = tax.GetReadOnlyDataBuffers();
            IEnumerable<ReadOnlyMemory<double>> readOnlyPrices = price.GetReadOnlyDataBuffers();
            IEnumerable<ReadOnlyMemory<double>> readOnlyDiscounts = discount.GetReadOnlyDataBuffers();

            IEnumerator<ReadOnlyMemory<double>> taxesEnumerator = readOnlyTaxes.GetEnumerator();
            IEnumerator<ReadOnlyMemory<double>> pricesEnumerator = readOnlyPrices.GetEnumerator();
            IEnumerator<ReadOnlyMemory<double>> discountsEnumerator = readOnlyDiscounts.GetEnumerator();

            while (taxesEnumerator.MoveNext() && pricesEnumerator.MoveNext() && discountsEnumerator.MoveNext())
            {
                ReadOnlySpan<double> taxes = taxesEnumerator.Current.Span;
                ReadOnlySpan<double> prices = pricesEnumerator.Current.Span;
                ReadOnlySpan<double> discounts = discountsEnumerator.Current.Span;
                for (int i = 0; i < prices.Length; ++i)
                {
                    ret[i] = (prices[i] * (1 - discounts[i]) * (1 + taxes[i]));
                }
            }

            return ret;
        }

        internal static PrimitiveColumn<double> ComputeDiscountPrice(PrimitiveColumn<double> price, PrimitiveColumn<double> discount)
        {
            if (price.Length != discount.Length)
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            PrimitiveColumn<double> ret = new PrimitiveColumn<double>("Prices", price.Length);

            IEnumerable<ReadOnlyMemory<double>> readOnlyPrices = price.GetReadOnlyDataBuffers();
            IEnumerable<ReadOnlyMemory<double>> readOnlyDiscounts = discount.GetReadOnlyDataBuffers();

            IEnumerator<ReadOnlyMemory<double>> pricesEnumerator = readOnlyPrices.GetEnumerator();
            IEnumerator<ReadOnlyMemory<double>> discountsEnumerator = readOnlyDiscounts.GetEnumerator();

            while (pricesEnumerator.MoveNext() && discountsEnumerator.MoveNext())
            {
                ReadOnlySpan<double> prices = pricesEnumerator.Current.Span;
                ReadOnlySpan<double> discounts = discountsEnumerator.Current.Span;
                for (int i = 0; i < prices.Length; ++i)
                {
                    ret[i] = (prices[i] * (1 - discounts[i]));
                }
            }

            return ret;
        }
    }
}
