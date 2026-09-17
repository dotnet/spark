# Variant schemas and Spark SQL

Spark 4.0 supports `VariantType` for semi-structured data, including objects, arrays and scalar JSON values. The current source supports Variant schemas and operations in Spark SQL; this is not available in the published Microsoft.Spark 2.3.1 package.

## Read JSON with an explicit schema

Given a JSON file containing one record per line:

```json
{"payload":{"id":7,"tags":["new"]}}
{"payload":{"id":8,"active":true}}
```

Describe `payload` with `VariantType`, then extract supported .NET types before collecting:

```csharp
using System.Collections.Generic;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

// spark is an existing SparkSession; path points to the JSON file.
var schema = new StructType(new[]
{
    new StructField("payload", new VariantType())
});
DataFrame data = spark.Read().Schema(schema).Json(path);

IEnumerable<Row> rows = data.SelectExpr(
    "variant_get(payload, '$.id', 'int') AS id",
    "to_json(payload) AS payload_json").Collect();
foreach (Row row in rows)
{
    int id = row.GetAs<int>("id");
    string json = row.GetAs<string>("payload_json");
}
```

`Schema()`, `Columns()` and `DTypes()` recognize Variant fields, including those nested in a `StructType`, an `ArrayType` or the values of a `MapType`.

## Use SQL expressions

Use the existing `SparkSession.Sql`, `Functions.Expr` and `DataFrame.SelectExpr` APIs. For example:

```csharp
DataFrame data = spark.Sql("SELECT parse_json('{\"id\":7}') AS payload");
IEnumerable<Row> rows = data.SelectExpr(
    "variant_get(payload, '$.id', 'int') AS id").Collect();
```

`parse_json` creates a Variant value in Spark. `variant_get` with a third argument converts the selected value to that SQL type; without it, the result is still Variant. `to_json` returns a JSON string. A missing path returns SQL NULL; an invalid conversion raises a Spark error unless the application explicitly uses `try_variant_get`.

JSON `null` is a Variant value, distinct from SQL NULL. For example, `is_variant_null(parse_json('null'))` is true, while `parse_json('null') IS NULL` is false. A missing Variant field in a JSON record produces SQL NULL.

## Scope

This support requires Spark 4.0 and its matching Scala bridge. It does not add a native .NET Variant value type, automatic `JsonObject` conversion, Variant values in `CreateDataFrame`, or Variant UDF/Arrow inputs and outputs. Raw Variant values returned by row retrieval have no supported .NET representation; do not depend on their internal binary/dictionary form. Project Variant columns to supported types in Spark SQL before retrieving rows into .NET.
