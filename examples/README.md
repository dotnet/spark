# .NET for Apache Spark Samples

[.NET for Apache Spark](https://dot.net/spark) is a free, open-source, and cross-platform big data analytics framework.

In the **examples** folder, we provide samples which will help you get started with .NET for Apache Spark
and demonstrate how to infuse big data analytics into existing and new .NET apps. 

There are two broad categories of .NET for Apache Spark samples:

* **[Microsoft.Spark.CSharp.Examples](Microsoft.Spark.CSharp.Examples):** Sample C# .NET for Apache Spark apps.

* **[Microsoft.Spark.FSharp.Examples](Microsoft.Spark.FSharp.Examples):** Sample F# .NET for Apache Spark apps.

**Note:** The samples in each of these folders fall under additional sub-categories, such as batch, streaming, and machine learning.

The launch examples use Spark 3.5.3 with Scala 2.12 and `microsoft-spark-3-5_2.12-<version>.jar`. Replace `<version>` with the .NET for Apache Spark release matching the application's NuGet package and Worker. Use a clean publish directory when upgrading from Spark 2.x. Spark 4.0 uses a separate Scala 2.13 bridge with JDK 17; check the APIs used by a sample before assuming it also runs on Spark 4.0. See the [migration guide](../docs/migration-guide.md#upgrading-after-spark-2x-support-removal).
