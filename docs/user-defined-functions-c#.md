# User-Defined Functions - C#
This documentation contains user-defined function (UDF) examples. It shows how to define UDFs and how to use UDFs with Row objects as examples.

## Pre-requisites:
Install Microsoft.Spark.Worker. When you want to execute a C# UDF, Spark needs to understand how to launch the .NET CLR to execute this UDF. Microsoft.Spark.Worker provides a collection of classes to Spark that enable this functionality. Please see more details at [how to install Microsoft.Spark.Worker](https://docs.microsoft.com/en-us/dotnet/spark/tutorials/get-started#5-install-net-for-apache-spark) and [how to deploy worker and UDF binaries](https://docs.microsoft.com/en-us/dotnet/spark/how-to-guides/deploy-worker-udf-binaries).

## UDF that takes in Row objects

```csharp
// Create DataFrame which will also be used in the following examples.
DataFrame df = spark.Range(0, 5).WithColumn("structId", Struct("id"));

// Define UDF that takes in Row objects
Func<Column, Column> udf1 = Udf<Row, int>(
    row => row.GetAs<int>(0) + 100);

// Use UDF with DataFrames
df.Select(udf(df["structId"])).Show();
```

## UDF that returns Row objects
Please note that `GenericRow` objects need to be used here.

```csharp
// Define UDF that returns Row objects
var schema = new StructType(new[]
{
    new StructField("col1", new IntegerType()),
    new StructField("col2", new StringType())
});            
Func<Column, Column> udf2 = Udf<int>(
    id => new GenericRow(new object[] { 1, "abc" }), schema);

// Use UDF with DataFrames
df.Select(udf(df["id"])).Show();
```

## Chained UDF with Row objects

```csharp
// Chained UDF using udf1 and udf2 defined above.
df.Select(udf1(udf2(df["id"]))).Show();
```
