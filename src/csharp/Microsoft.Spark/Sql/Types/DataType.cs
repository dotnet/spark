// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Globalization;
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

        private static readonly Lazy<string[]> s_simpleTypeNormalizedNames =
            new Lazy<string[]>(
                () => s_simpleTypes.Select(t => NormalizeTypeName(t.Name)).ToArray());

        private static readonly Lazy<string[]> s_complexTypeNormalizedNames =
            new Lazy<string[]>(
                () => s_complexTypes.Select(t => NormalizeTypeName(t.Name)).ToArray());

        private string _typeName;

        /// <summary>
        /// Normalized type name.
        /// </summary>
        public string TypeName => _typeName ??= NormalizeTypeName(GetType().Name);

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
        /// Parses a JSON string to create a <see cref="JvmObjectReference"/>.
        /// It references a <see cref="StructType"/> on the JVM side.
        /// </summary>
        /// <param name="jvm">JVM bridge to use</param>
        /// <param name="json">JSON string to parse</param>
        /// <returns>The new JvmObjectReference created from the JSON string</returns>
        internal static JvmObjectReference FromJson(IJvmBridge jvm, string json)
        {
            return (JvmObjectReference)jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.types.DataType",
                "fromJson",
                json);
        }

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
                    string typeName = type.ToString();

                    int typeIndex = Array.IndexOf(s_complexTypeNormalizedNames.Value, typeName);

                    if (typeIndex != -1)
                    {
                        return (DataType)Activator.CreateInstance(
                            s_complexTypes[typeIndex],
                            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                            null,
                            new object[] { typeJObject },
                            null);
                    }
                    else if (typeName == "udt")
                    {
                        if (typeJObject.TryGetValue("class", out JToken classToken))
                        {
                            if (typeJObject.TryGetValue("sqlType", out JToken sqlTypeToken))
                            {
                                return new StructType((JObject)sqlTypeToken);
                            }
                        }

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
        /// Does this type need to conversion between C# object and internal SQL object.
        /// </summary>
        internal virtual bool NeedConversion() => false;

        /// <summary>
        /// Converts an internal SQL object into a native C# object.
        /// </summary>
        internal virtual object FromInternal(object obj) => obj;

        /// <summary>
        /// Parses a JToken object that represents a simple type.
        /// </summary>
        /// <param name="json">JToken object to parse</param>
        /// <returns>The new DataType instance from the JSON string</returns>
        private static DataType ParseSimpleType(JToken json)
        {
            string typeName = json.ToString();

            int typeIndex = Array.IndexOf(s_simpleTypeNormalizedNames.Value, typeName);

            if (typeIndex != -1)
            {
                return (DataType)Activator.CreateInstance(s_simpleTypes[typeIndex]);
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
        private static string NormalizeTypeName(string typeName)
        {
#if !NETSTANDARD2_0
            return string.Create(typeName.Length - 4, typeName, (span, name) =>
            {
                name.AsSpan(0, name.Length - 4).ToLower(span, CultureInfo.CurrentCulture);
            });
#else
            return typeName.Substring(0, typeName.Length - 4).ToLower();
#endif
        }
    }
}
