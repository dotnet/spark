# Guide to User-Defined Functions (UDF)

This is a guide to show how to use UDFs in .NET for Apache Spark.

## What are UDFs

[User-Defined Functions (UDFs)](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/expressions/UserDefinedFunction.html) are a feature of Spark that allow developers to use custom functions to extend the vocabulary of Spark SQL’s Domain Specific Language. They transform values from a single row within a table to produce a single corresponding output value per row based on the logic defined in the UDF.

Let's take the following as an example for a UDF definition:

```csharp
string s1 = "hello";
Func<Column, Column> udf = Udf<string, string>(
    str => $"{s1} {str}");

```

And for a sample Dataframe, let's take the following Dataframe `df`:

```text
    +-------+
    |   name|
    +-------+
    |Michael|
    |   Andy|
    | Justin|
    +-------+
```

Now let's apply the above defined `udf` to the dataframe `df`:

```csharp
DataFrame udfResult = df.Select(udf(df["name"]));
```

This would return the below as the Dataframe `udfResult`:

```text
    +-------------+
    |         name|
    +-------------+
    |hello Michael|
    |   hello Andy|
    | hello Justin|
    +-------------+
```

## Good to know while implementing UDFs

One behavior to be aware of while implementing UDFs in .NET for Apache Spark is the way target data or data being used in the UDF gets picked up for serialization. .NET for Apache Spark uses reflection to identify the data referenced in a UDF in order to serialize it while sending to Apache Spark. It is recommended when defining multiple UDFs in a common scope, to restrict the visibility of the data/variables being referenced in a UDF to the scope of only that UDF, such that it is not visible from any other UDFs defined in the uber scope.

Let's take the following simple code snippet to illustrate what that means:

```csharp
{
    string s1 = "variable 1";
    Func<Column, Column> udf1 = Udf<string, string>(
        str => $"{str}: {s1}");
}

{
    string s2 = "variable 2";
    Func<Column, Column> udf2 = Udf<string, string>(
        str => $"{str}: {s2}");
}
```

As can be seen in the above example, `s1` is accessible to only `udf1` whereas `s2` only to `udf2`. This prevents reflection from picking up any more than the referenced data in the UDF during its serialization.

The following is NOT recommended:

```csharp
string s1 = "variable 1";
string s2 = "variable 2";
{
    Func<Column, Column> udf1 = Udf<string, string>(
        str => $"{str}: {s1}");
}

{
    Func<Column, Column> udf2 = Udf<string, string>(
        str => $"{str}: {s2}");
}
```

In the above example where the scope of `s1` and `s2` is accessible to both the UDFs, both `s1` and `s2` get picked for serialization as target data while serializing both UDFs. This is not ideal, as we should not be serializing data that is not being referenced in the UDF.