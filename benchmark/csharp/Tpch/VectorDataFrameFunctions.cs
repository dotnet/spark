// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Data.Analysis;

namespace Tpch
{
    internal static class VectorDataFrameFunctions
    {
        internal static PrimitiveDataFrameColumn<double> ComputeTotal(PrimitiveDataFrameColumn<double> price, PrimitiveDataFrameColumn<double> discount, PrimitiveDataFrameColumn<double> tax)
        {
            if ((price.Length != discount.Length) || (price.Length != tax.Length))
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            return (PrimitiveDataFrameColumn<double>)(price * (1 - discount) * (1 + tax));
        }

        internal static PrimitiveDataFrameColumn<double> ComputeDiscountPrice(PrimitiveDataFrameColumn<double> price, PrimitiveDataFrameColumn<double> discount)
        {
            if (price.Length != discount.Length)
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            return (PrimitiveDataFrameColumn<double>)(price * (1 - discount));
        }
    }
}
