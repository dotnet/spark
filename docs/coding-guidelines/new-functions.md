Minium requirements for implementing Spark API functions
======================================================

This document describes the minimum requirements for contributing new Spark API functions to dotnet/spark.

Comments
--------

Each Spark API has a comment that is used to generate the documentation on the [.NET API Browser](https://docs.microsoft.com/en-gb/dotnet/api/?view=spark-dotnet). The recommended approach is to start with the comments that are used by the Apache Spark comments for each function and then expand when required.

To find the original Apache Spark function's comment you can either go directly to the source code such as [Column Apache Spark implementation](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/Column.scala) or you can view the [Apache Spark API documentation](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column).

Unit Tests
----------

The approach for unit testing is to check whether the functions have been implemented correctly in dotnet and not to verify that the actual Apache Spark implementation is correct. This approach means that each function should have a unit test that verifies that the:

* Function name is correct
* Number of parameters are correct
* Parameters are of the correct type
* Function return is of the correct type

The typical approach is to use `Assert.IsType<>` and call a function:

 
    ```C#
    Assert.IsType<Column>(Col("col2"));
    ```

For these unit tests see [the IPC Functions Tests](https://github.com/dotnet/spark/blob/master/src/csharp/Microsoft.Spark.E2ETest/IpcTests/Sql/FunctionsTests.cs#L41).