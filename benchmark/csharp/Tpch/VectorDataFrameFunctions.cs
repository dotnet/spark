// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Microsoft.Data.Analysis;

namespace Tpch
{
    internal static class VectorDataFrameFunctions
    {
        internal static DoubleDataFrameColumn ComputeTotal(DoubleDataFrameColumn price, DoubleDataFrameColumn discount, DoubleDataFrameColumn tax)
        {
            if ((price.Length != discount.Length) || (price.Length != tax.Length))
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            return price * (1 - discount) * (1 + tax);
        }

        internal static DoubleDataFrameColumn ComputeDiscountPrice(DoubleDataFrameColumn price, DoubleDataFrameColumn discount)
        {
            if (price.Length != discount.Length)
            {
                throw new ArgumentException("Arrays need to be the same length");
            }

            return price * (1 - discount);
        }
    }
}
