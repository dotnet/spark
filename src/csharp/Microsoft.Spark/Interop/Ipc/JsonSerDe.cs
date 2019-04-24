// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Linq;
using Newtonsoft.Json.Linq;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Json.NET Serialization/Deserialization helper class.
    /// </summary>
    internal static class JsonSerDe
    {
        /// Note: Scala side uses JSortedObject when parsing JSON, so the properties
        /// in JObject need to be sorted.
        /// <summary>
        /// Extension method to sort items in a JSON object by keys.
        /// </summary>
        /// <param name="jObject"></param>
        /// <returns></returns>
        public static JObject SortProperties(this JObject jObject)
        {
            var sortedJObject = new JObject();
            foreach (JProperty property in jObject.Properties().OrderBy(p => p.Name))
            {
                if (property.Value is JObject elem)
                {
                    sortedJObject.Add(property.Name, elem.SortProperties());
                }
                else if (property.Value is JArray arrayElem)
                {
                    sortedJObject.Add(property.Name, arrayElem.SortProperties());
                }
                else
                {
                    sortedJObject.Add(property.Name, property.Value);
                }
            }

            return sortedJObject;
        }

        /// <summary>
        /// Extend method to sort items in a JSON array by keys.
        /// </summary>
        public static JArray SortProperties(this JArray jArray)
        {
            if (jArray.Count == 0)
            {
                return jArray;
            }

            var sortedJArray = new JArray();
            foreach (JToken item in jArray)
            {
                if (item is JObject elem)
                {
                    sortedJArray.Add(elem.SortProperties());
                }
                else if (item is JArray arrayElem)
                {
                    sortedJArray.Add(arrayElem.SortProperties());
                }
            }

            return sortedJArray;
        }
    }

}
