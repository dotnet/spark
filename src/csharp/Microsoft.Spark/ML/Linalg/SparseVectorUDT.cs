// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using Newtonsoft.Json.Linq;

namespace Microsoft.Spark.ML.Linalg
{
    /// <summary>
    /// The data type for a SparseVectorUDT, used by SparseVector. 
    /// </summary>
    public class SparseVectorUDT : DataType
    {
        /// <summary>
        /// Returns JSON object describing this type.
        /// </summary>
        internal override object JsonValue =>
            new JObject(
                new JProperty("type", "udt"),
                new JProperty("class", "org.apache.spark.ml.linalg.VectorUDT"),
                new JProperty("pyClass", "pyspark.ml.linalg.VectorUDT"),
                new JProperty("sqlType", new JObject(
                    new JProperty("type", "struct"),
                    new JProperty("fields", new JArray(
                        new JObject(
                            new JObject("metadata"),
                            new JProperty("name", "type"),
                            new JProperty("type", "byte"),
                            new JProperty("nullable", false)
                            ),
                        new JObject(
                            new JObject("metadata"),
                            new JProperty("name", "size"),
                            new JProperty("type", "integer"),
                            new JProperty("nullable", false)
                            ),
                        new JObject(
                            new JObject("metadata"),
                            new JProperty("name", "indices"),
                            new JProperty("type", new JObject(
                                new JProperty("type", "array"),
                                new JProperty("elementType", "integer"),
                                new JProperty("containsNull", false))),
                            new JProperty("nullable", true)
                            ),
                        new JObject(
                            new JObject("metadata"),
                            new JProperty("name", "values"),
                            new JProperty("type", new JObject(
                                new JProperty("type", "array"),
                                new JProperty("elementType", "double"),
                                new JProperty("containsNull", false))),
                            new JProperty("nullable", true)
                            )
                        ))
                    ))
                );

        /// <summary>
        /// Returns a readable string that represents this type.
        /// </summary>
        public override string SimpleString => "vector";
    }
}
