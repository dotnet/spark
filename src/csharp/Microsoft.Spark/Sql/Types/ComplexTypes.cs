// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.Interop.Ipc;
using Newtonsoft.Json.Linq;

namespace Microsoft.Spark.Sql.Types
{
    /// <summary>
    /// An array type containing multiple values of a type.
    /// </summary>
    public sealed class ArrayType : DataType
    {
        /// <summary>
        /// Constructor for ArrayType class.
        /// </summary>
        /// <param name="elementType">The data type of elements in this array</param>
        /// <param name="containsNull">Indicates if elements can be null</param>
        public ArrayType(DataType elementType, bool containsNull = true)
        {
            ElementType = elementType;
            ContainsNull = containsNull;
        }

        /// <summary>
        /// Constructor for ArrayType class.
        /// </summary>
        /// <param name="json">JSON object to create the array type from</param>
        internal ArrayType(JObject json) => FromJson(json);

        /// <summary>
        /// Returns the data type of the elements in an array.
        /// </summary>
        public DataType ElementType { get; private set; }

        /// <summary>
        /// Checks if the array can contain null values.
        /// </summary>
        public bool ContainsNull { get; private set; }

        /// <summary>
        /// Readable string representation for this type.
        /// </summary>
        public override string SimpleString =>
            string.Format("array<{0}>", ElementType.SimpleString);

        /// <summary>
        /// Returns JSON object describing this type.
        /// </summary>
        internal override object JsonValue =>
            new JObject(
                new JProperty("type", TypeName),
                new JProperty("elementType", ElementType.JsonValue),
                new JProperty("containsNull", ContainsNull));

        /// <summary>
        /// Constructs a ArrayType object from a JSON object.
        /// </summary>
        /// <param name="json">JSON object used to construct a ArrayType object</param>
        /// <returns>ArrayType object</returns>
        private DataType FromJson(JObject json)
        {
            ElementType = ParseDataType(json["elementType"]);
            ContainsNull = (bool)json["containsNull"];
            return this;
        }

        internal override bool NeedConversion() => ElementType.NeedConversion();

        internal override object FromInternal(object obj)
        {
            if (!NeedConversion() || obj == null)
            {
                return obj;
            }
            
            var arrayList = (ArrayList)obj;
            for (int i = 0; i < arrayList.Count; ++i)
            {
                arrayList[i] = ElementType.FromInternal(arrayList[i]);
            }

            return arrayList;
        }
    }

    /// <summary>
    /// The data type for a map. 
    /// </summary>
    public sealed class MapType : DataType
    {
        /// <summary>
        /// Constructor for MapType class.
        /// </summary>
        /// <param name="keyType">The data type of keys in this map</param>
        /// <param name="valueType">The data type of values in this map</param>
        /// <param name="valueContainsNull">Indicates if values can be null</param>
        public MapType(DataType keyType, DataType valueType, bool valueContainsNull = true)
        {
            KeyType = keyType;
            ValueType = valueType;
            ValueContainsNull = valueContainsNull;
        }

        /// <summary>
        /// Constructor for MapType class.
        /// </summary>
        /// <param name="json">JSON object to create the map type from</param>
        internal MapType(JObject json) => FromJson(json);

        /// <summary>
        /// Returns the data type of the keys in the map.
        /// </summary>
        public DataType KeyType { get; private set; }

        /// <summary>
        /// Returns the data type of the values in the map.
        /// </summary>
        public DataType ValueType { get; private set; }

        /// <summary>
        /// Checks if the value can contain null values.
        /// </summary>
        public bool ValueContainsNull { get; private set; }

        /// <summary>
        /// Readable string representation for this type.
        /// </summary>
        public override string SimpleString =>
            string.Format("map<{0},{1}>", KeyType.SimpleString, ValueType.SimpleString);

        /// <summary>
        /// Returns JSON object describing this type.
        /// </summary>
        internal override object JsonValue =>
            new JObject(
                new JProperty("type", TypeName),
                new JProperty("keyType", KeyType.JsonValue),
                new JProperty("valueType", ValueType.JsonValue),
                new JProperty("valueContainsNull", ValueContainsNull));

        /// <summary>
        /// Constructs a MapType object from a JSON object.
        /// </summary>
        /// <param name="json">JSON object used to construct a MapType object</param>
        /// <returns>MapType object</returns>
        private DataType FromJson(JObject json)
        {
            KeyType = ParseDataType(json["keyType"]);
            ValueType = ParseDataType(json["valueType"]);
            ValueContainsNull = (bool)json["valueContainsNull"];
            return this;
        }

        internal override bool NeedConversion() =>
            KeyType.NeedConversion() || ValueType.NeedConversion();

        internal override object FromInternal(object obj)
        {
            if (!NeedConversion() || obj == null)
            {
                return obj;
            }

            var hashTable = (Hashtable)obj;
            var convertedHashtable = new Hashtable(hashTable.Count);
            foreach (DictionaryEntry entry in hashTable)
            {
                convertedHashtable[KeyType.FromInternal(entry.Key)] =
                    ValueType.FromInternal(entry.Value);
            }

            return convertedHashtable;
        }
    }

    /// <summary>
    /// A type that represents a field inside StructType.
    /// </summary>
    public sealed class StructField
    {
        /// <summary>
        /// Constructor for StructFieldType class.
        /// </summary>
        /// <param name="name">The name of this field</param>
        /// <param name="dataType">The data type of this field</param>
        /// <param name="isNullable">Indicates if values of this field can be null</param>
        /// <param name="metadata">The metadata of this field</param>
        public StructField(
            string name,
            DataType dataType,
            bool isNullable = true,
            JObject metadata = null)
        {
            Name = name;
            DataType = dataType;
            IsNullable = isNullable;
            Metadata = metadata ?? new JObject();
        }

        /// <summary>
        /// Constructor for StructFieldType class.
        /// </summary>
        /// <param name="json">JSON object to construct a StructFieldType object</param>
        internal StructField(JObject json)
        {
            Name = json["name"].ToString();
            DataType = DataType.ParseDataType(json["type"]);
            IsNullable = (bool)json["nullable"];
            Metadata = (JObject)json["metadata"];
        }

        /// <summary>
        /// The name of this field.
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// The data type of this field.
        /// </summary>
        public DataType DataType { get; }

        /// <summary>
        /// Checks if values of this field can be null.
        /// </summary>
        public bool IsNullable { get; }

        /// <summary>
        /// The metadata of this field.
        /// </summary>
        internal JObject Metadata { get; }

        /// <summary>
        /// Returns a readable string that represents this type.
        /// </summary>
        public override string ToString() => $"StructField({Name},{DataType.SimpleString})";

        /// <summary>
        /// Returns JSON object describing this type.
        /// </summary>
        internal object JsonValue =>
            new JObject(
                new JProperty("name", Name),
                new JProperty("type", DataType.JsonValue),
                new JProperty("nullable", IsNullable),
                new JProperty("metadata", Metadata));
    }

    /// <summary>
    /// Struct type represents a struct with multiple fields.
    /// This type is also used to represent a Row object in Spark.
    /// </summary>
    public sealed class StructType : DataType
    {
        /// <summary>
        /// Constructor for StructType class.
        /// </summary>
        /// <param name="json">JSON object to construct a StructType object</param>
        internal StructType(JObject json) => FromJson(json);

        /// <summary>
        /// Constructor for StructType class.
        /// </summary>
        /// <param name="jvmObject">StructType object on JVM</param>
        internal StructType(JvmObjectReference jvmObject) =>
            FromJson(JObject.Parse((string)jvmObject.Invoke("json")));

        /// <summary>
        /// Returns a list of StructFieldType objects.
        /// </summary>
        public List<StructField> Fields { get; private set; }

        /// <summary>
        /// Constructor for StructType class.
        /// </summary>
        /// <param name="fields">A collection of StructFieldType objects</param>
        public StructType(IEnumerable<StructField> fields)
        {
            Fields = fields.ToList();
        }

        /// <summary>
        /// Returns a readable string that represents this type.
        /// </summary>
        public override string SimpleString =>
            $"struct<{string.Join(",", Fields.Select(f => $"{f.Name}:{f.DataType.SimpleString}"))}>";

        /// <summary>
        /// Returns JSON object describing this type.
        /// </summary>
        internal override object JsonValue =>
            new JObject(
                new JProperty("type", TypeName),
                new JProperty("fields", Fields.Select(f => f.JsonValue).ToArray()));

        /// <summary>
        /// Constructs a StructType object from a JSON object
        /// </summary>
        /// <param name="json">JSON object used to construct a StructType object</param>
        /// <returns>A StuructType object</returns>
        private DataType FromJson(JObject json)
        {
            IEnumerable<JObject> fieldsJObjects = json["fields"].Select(f => (JObject)f);
            Fields = fieldsJObjects.Select(
                fieldJObject => new StructField(fieldJObject)).ToList();
            return this;
        }
    }
}
