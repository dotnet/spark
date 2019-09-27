Minimum Requirements for Implementing Spark API Functions
======================================================

This document describes the minimum requirements for contributing new Spark API functions to [dotnet/spark](https://github.com/dotnet/spark).

Comments
--------

Each Spark API has a comment that is used to generate the documentation on the [.NET API Browser](https://docs.microsoft.com/en-gb/dotnet/api/?view=spark-dotnet). The recommended approach is to start with comments that are used by the respective API you are attempting to expose from the Apache Spark code and then improvise when required. For instance, see the comments for the original implementation of `agg` in [Scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/RelationalGroupedDataset.scala#L223) and compare against [C#](https://github.com/dotnet/spark/blob/master/src/csharp/Microsoft.Spark/Sql/RelationalGroupedDataset.cs#L37).

To find the original Apache Spark's comment for an API, you can either go directly to the source code such as [Column.scala](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/Column.scala) in Apache Spark Core or you can view the respective [Column documentation](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column) on the Apache Spark website.

Unit Tests
----------

The approach for unit testing is to check whether the functions have been implemented correctly in .NET and not to verify that the actual Apache Spark implementation is correct (since this would have been tested in Apache Spark Core already). This approach means that each function should have a unit test that verifies that the following is satisfied:

* Function name is correct
* Number of parameters are correct
* Parameters are of the correct type
* Function return is of the correct type

The typical approach is to use `Assert.IsType<>` and call a function:


```C#
Assert.IsType<Column>(Col("col2"));
```

For these unit tests see [the IPC Functions Tests](https://github.com/dotnet/spark/blob/master/src/csharp/Microsoft.Spark.E2ETest/IpcTests/Sql/FunctionsTests.cs#L41).
