// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.IO;
using System.Linq;
using System.Text.Json;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// System.Text.Json Serialization/Deserialization helper class.
    /// </summary>
    internal static class JsonSerDe
    {
        /// Note: Scala side uses JSortedObject when parsing JSON, so the properties
        /// in JsonElement need to be sorted.
        /// <summary>
        /// Extension method to sort items in a JSON object by keys.
        /// </summary>
        /// <param name="jObject"></param>
        /// <returns></returns>
        public static JsonElement SortProperties(this JsonElement jObject)
        {
            using (var stream = new MemoryStream())
            {
                using (var writer = new Utf8JsonWriter(stream))
                {
                    writer.WriteStartObject();
                    foreach (JsonProperty prop in jObject.EnumerateObject().OrderBy(p => p.Name))
                    {
                        writer.WritePropertyName(prop.Name);
                        if (prop.Value.ValueKind == JsonValueKind.Object)
                        {
                            prop.Value.SortProperties().WriteTo(writer);
                        }
                        else if (prop.Value.ValueKind == JsonValueKind.Array)
                        {
                            prop.Value.SortArrayProperties().WriteTo(writer);
                        }
                        else
                        {
                            prop.Value.WriteTo(writer);
                        }
                    }
                    writer.WriteEndObject();
                    writer.Flush();               
                    return JsonDocument.Parse(stream.ToArray()).RootElement;
                }
            }
        }

        /// <summary>
        /// Extend method to sort items in a JSON array by keys.
        /// </summary>
        public static JsonElement SortArrayProperties(this JsonElement jArray)
        {
            if (jArray.GetArrayLength() == 0)
            {
                return jArray;
            }

            using (var stream = new MemoryStream())
            {
                using (var writer = new Utf8JsonWriter(stream))
                {
                    writer.WriteStartArray();
                    foreach (JsonElement item in jArray.EnumerateArray())
                    {                     
                        if (item.ValueKind == JsonValueKind.Object)
                        {
                            item.SortProperties().WriteTo(writer);
                        }
                        else if (item.ValueKind == JsonValueKind.Array)
                        {
                            item.SortArrayProperties().WriteTo(writer);
                        }
                        // TODO: is this required?
                        // else
                        // {
                        //     item.WriteTo(writer);
                        // }                        
                    }
                    writer.WriteEndArray();
                    writer.Flush();
                    return JsonDocument.Parse(stream.ToArray()).RootElement;
                }
            }
        }
    }

}
