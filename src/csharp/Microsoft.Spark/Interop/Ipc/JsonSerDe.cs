// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
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
        /// Extension method to sort items in a JSON object by keys. Please note that this 
        /// method uses recursion and shouldn't be used with untrusted/unbounded input data.
        /// </summary>
        /// <param name="jsonElement"></param>
        /// <returns></returns>
        public static string SortProperties(this JsonElement jsonElement)
        {
            var output = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(output))
            {
                jsonElement.SortPropertiesCore(writer);
            }
            return Encoding.UTF8.GetString(output.WrittenSpan.ToArray());
        }

        /// <summary>
        /// Helper method to Parse a string into a JsonElement.
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static JsonElement Parse(string json)
        {
            using (var document = JsonDocument.Parse(json))
            {
                return document.RootElement.Clone();
            }
        }           

        /// <summary>
        /// Helper method to Parse an object into a JsonElement.
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static JsonElement Parse(object obj)
        {
            return Parse(JsonSerializer.SerializeToUtf8Bytes(obj));
        }

        /// <summary>
        /// Helper method to Parse a byte array in UTF8 to a JsonElement.
        /// </summary>
        private static JsonElement Parse(byte[] utf8Json)
        {
            using (var document = JsonDocument.Parse(utf8Json))
            {
                return document.RootElement.Clone();
            }
        }        

        /// <summary>
        /// Sort all types of Json properties.
        /// </summary>
        private static void SortPropertiesCore(this JsonElement jsonElement, Utf8JsonWriter writer)
        {
            switch (jsonElement.ValueKind)
            {
                case JsonValueKind.Undefined:
                    throw new InvalidOperationException();
                case JsonValueKind.Object:
                    jsonElement.SortObjectProperties(writer);
                    break;
                case JsonValueKind.Array:
                    jsonElement.SortArrayProperties(writer);
                    break;
                case JsonValueKind.String:
                case JsonValueKind.Number:
                case JsonValueKind.True:
                case JsonValueKind.False:
                case JsonValueKind.Null:
                    jsonElement.WriteTo(writer);
                    break;
            };
        }

        /// <summary>
        /// Sort properties that are Json objects.
        /// </summary>
        private static void SortObjectProperties(this JsonElement jObject, Utf8JsonWriter writer)
        {
            Debug.Assert(jObject.ValueKind == JsonValueKind.Object);

            var propertyNames = new List<string>();
            foreach (JsonProperty prop in jObject.EnumerateObject())
            {
                propertyNames.Add(prop.Name);
            }
            propertyNames.Sort();

            writer.WriteStartObject();
            foreach (string name in propertyNames)
            {
                writer.WritePropertyName(name);
                jObject.GetProperty(name).WriteElementHelper(writer);
            }
            writer.WriteEndObject();
        }  

        /// <summary>
        /// Extend method to sort items in a JSON array by keys.
        /// </summary>
        private static void SortArrayProperties(this JsonElement jArray, Utf8JsonWriter writer)
        {
            Debug.Assert(jArray.ValueKind == JsonValueKind.Array);

            writer.WriteStartArray();
            foreach (JsonElement item in jArray.EnumerateArray())
            {
                item.WriteElementHelper(writer);
            }
            writer.WriteEndArray();
        }

        /// <summary>
        /// Helper to appropriately write Json types based on their kind.
        /// </summary>
        private static void WriteElementHelper(this JsonElement item, Utf8JsonWriter writer)
        {
            Debug.Assert(item.ValueKind != JsonValueKind.Undefined);

            if (item.ValueKind == JsonValueKind.Object)
            {
                item.SortObjectProperties(writer);
            }
            else if (item.ValueKind == JsonValueKind.Array)
            {
                item.SortArrayProperties(writer);
            }
            else
            {
                item.WriteTo(writer);
            }
        }
    }
}
