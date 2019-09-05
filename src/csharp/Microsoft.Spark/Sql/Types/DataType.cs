// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.Spark.Interop.Ipc;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Spark.Sql.Types
{
    /// <summary>
    /// The base type of all Spark SQL data types.
    /// Note that the implementation mirrors PySpark: spark/python/pyspark/sql/types.py
    /// The Scala version is spark/sql/catalyst/src/main/scala/org/apache/spark/sql/types/*.
    /// </summary>
    public abstract class DataType
    {
        private static readonly Type[] s_simpleTypes = new[] {
            typeof(NullType),
            typeof(StringType),
            typeof(BinaryType),
            typeof(BooleanType),
            typeof(DateType),
            typeof(TimestampType),
            typeof(DoubleType),
            typeof(FloatType),
            typeof(ByteType),
            typeof(IntegerType),
            typeof(LongType),
            typeof(ShortType),
            typeof(DecimalType) };

        private static readonly Type[] s_complexTypes = new[] {
            typeof(ArrayType),
            typeof(MapType),
            typeof(StructType) };

        /// <summary>
        /// Normalized type name.
        /// </summary>
        public string TypeName => NormalizeTypeName(GetType().Name);

        /// <summary>
        /// Simple string version of the current data type.
        /// </summary>
        public virtual string SimpleString => TypeName;

        /// <summary>
        /// The compact JSON representation of this data type.
        /// </summary>
        public string Json
        {
            get
            {
                object jObject = (JsonValue is JObject) ?
                    ((JObject)JsonValue).SortProperties() :
                    JsonValue;
                return JsonConvert.SerializeObject(jObject, Formatting.None);
            }
        }

        /// <summary>
        /// JSON value of this data type.
        /// </summary>
        internal virtual object JsonValue => TypeName;

        /// <summary>
        /// Parses a JSON string to construct a DataType.
        /// </summary>
        /// <param name="json">JSON string to parse</param>
        /// <returns>The new DataType instance from the JSON string</returns>
        public static DataType ParseDataType(string json) => ParseDataType(JToken.Parse(json));

        /// <summary>
        /// Checks if the given object is same as the current object by
        /// checking the string version of this type.
        /// </summary>
        /// <param name="obj">Other object to compare against</param>
        /// <returns>True if the other object is equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj is DataType otherDataType)
            {
                return SimpleString == otherDataType.SimpleString;
            }

            return false;
        }

        /// <summary>
        /// Returns the hash code of the current object.
        /// </summary>
        /// <returns>The hash code of the current object</returns>
        public override int GetHashCode() => SimpleString.GetHashCode();

        /// <summary>
        /// Parses a JToken object to construct a DataType.
        /// </summary>
        /// <param name="json">JToken object to parse</param>
        /// <returns>The new DataType instance from the JSON string</returns>
        internal static DataType ParseDataType(JToken json)
        {
            if (json.Type == JTokenType.Object)
            {
                var typeJObject = (JObject)json;
                if (typeJObject.TryGetValue("type", out JToken type))
                {
                    Type complexType = s_complexTypes.FirstOrDefault(
                        (t) => NormalizeTypeName(t.Name) == type.ToString());

                    if (complexType != default)
                    {
                        return (DataType)Activator.CreateInstance(
                            complexType,
                            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                            null,
                            new object[] { typeJObject },
                            null);
                    }
                    else if (type.ToString() == "udt")
                    {
                        throw new NotImplementedException();
                    }
                }

                throw new ArgumentException($"Could not parse data type: {type}");
            }
            else
            {
                return ParseSimpleType(json);
            }

        }

        /// <summary>
        /// Parses a JToken object that represents a simple type.
        /// </summary>
        /// <param name="json">JToken object to parse</param>
        /// <returns>The new DataType instance from the JSON string</returns>
        private static DataType ParseSimpleType(JToken json)
        {
            string typeName = json.ToString();
            Type simpleType = s_simpleTypes.FirstOrDefault(
                (t) => NormalizeTypeName(t.Name) == typeName);

            if (simpleType != default)
            {
                return (DataType)Activator.CreateInstance(simpleType);
            }

            Match decimalMatch = DecimalType.s_fixedDecimal.Match(typeName);
            if (decimalMatch.Success)
            {
                return new DecimalType(
                    int.Parse(decimalMatch.Groups[1].Value),
                    int.Parse(decimalMatch.Groups[2].Value));
            }

            throw new ArgumentException($"Could not parse data type: {json}");
        }

        /// <summary>
        /// Remove "Type" from the end of type name and lower cases to align with Scala type name.
        /// </summary>
        /// <param name="typeName">Type name to normalize</param>
        /// <returns>Normalized type name</returns>
        private static string NormalizeTypeName(string typeName) =>
            typeName.Substring(0, typeName.Length - 4).ToLower();
    }
}
