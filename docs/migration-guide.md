# Migration Guide

- [Upgrading after Spark 2.x support removal](#upgrading-after-spark-2x-support-removal)
- [Upgrading from Microsoft.Spark 0.x to 1.0](#upgrading-from-microsoftspark-0x-to-10)

## Upgrading after Spark 2.x support removal

The next release no longer runs on Apache Spark 2.x or ships a Scala 2.11 bridge. Spark 3.0 through 3.5 remain in scope. Spark 4.0 integration uses a separate Scala 2.13 bridge and JDK 17; it is not a claim of full API, extension, REPL, or deployment parity. Check the APIs and configuration used by your application before changing runtimes.

For an application that must stay on Spark 2.x, keep its existing compatible `Microsoft.Spark`, Worker, and bridge release together. The [v2.3.1 release notes](release-notes/2.3.1/release-2.3.1.md) record that release's compatibility; removal from the next release does not change historical releases.

### Match the runtime and deploy clean output

1. Choose a retained runtime and its matching bridge. The current examples use Spark 3.5.3, Scala 2.12, and `microsoft-spark-3-5_2.12-<version>.jar`. Spark 4.0 uses `microsoft-spark-4-0_2.13-<version>.jar` with Scala 2.13 and JDK 17. `<version>` is the .NET for Apache Spark release, not the Apache Spark version. Use the matching `Microsoft.Spark` NuGet package and Worker release on the driver and executors.
2. If building from a checkout that previously built Spark 2.x, remove only its obsolete generated `src/scala/microsoft-spark-2-4/target` output after checking the resolved path. Removing a Maven module does not clean its old `target` directory. Preserve source files, logs, and unrelated work.
3. Clean the application's generated build output and publish into a new, empty directory. `dotnet clean` alone may leave manually copied JARs behind. Inspect the resulting package, publish directory, Spark `jars` directories, and explicit classpaths for old `microsoft-spark-2-*.jar`, `microsoft-spark-2.*.jar`, and Scala 2.11 connector/bridge JARs. Do not overlay a new deployment onto an old publish directory or delete unrelated dependency JARs by a broad wildcard.
4. Update `spark-submit`, cluster-installed bridge paths, and connector coordinates to the selected Spark and Scala binary versions. For the Spark 3.5.3 example, the Kafka connector is `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`. Check driver and executor artifacts together, then validate the packaged application on the target cluster. Reusing a Spark 2.x streaming checkpoint requires a separate supported migration assessment; a clean application build does not establish checkpoint compatibility.

The [Windows](building/windows-instructions.md#building-spark-net-scala-extensions-layer) and [Ubuntu](building/ubuntu-instructions.md#building-spark-net-scala-extensions-layer) build guides keep the JDK 8 Spark 3.x reactor and the independent JDK 17 Spark 4.0 build separate.

### Removed window-boundary APIs

Five APIs that targeted Spark 2.x are removed. Recompile callers using the existing `long`-based range overloads:

| Removed API | Replacement |
| --- | --- |
| `Window.RangeBetween(Column, Column)` | `Window.RangeBetween(long, long)` |
| `WindowSpec.RangeBetween(Column, Column)` | `WindowSpec.RangeBetween(long, long)` |
| `Functions.UnboundedPreceding()` | `Window.UnboundedPreceding` |
| `Functions.UnboundedFollowing()` | `Window.UnboundedFollowing` |
| `Functions.CurrentRow()` | `Window.CurrentRow` |

The `Window` boundary members are properties returning `long`, not methods returning `Column`. They are replacements for range-boundary construction, not drop-in replacements wherever a `Column` is expected. For example:

```csharp
using Microsoft.Spark.Sql.Expressions;

var window = Window.OrderBy("key")
    .RangeBetween(Window.UnboundedPreceding, Window.CurrentRow);
```

For a bounded range, pass numeric offsets such as `-5L` and `0L` to the `long` overload. API `Since` annotations still record when an API was introduced; an annotation mentioning Spark 2.x does not mean a retained API should be removed.

### Retired HDInsight notebook installer

The experimental Spark 2.4/HDInsight 4.0 notebook installer is retired. Its patched Livy, Scala 2.11, SparkMagic, and old REPL binaries have not been migrated or validated for Spark 3.x or 4.0. See the [retirement notice](../deployment/HDI-Spark/Notebooks/README.md) for the historical instructions. The generic Worker deployment scripts are not a replacement for that notebook integration.

## Upgrading from Microsoft.Spark 0.x to 1.0
- Limited support for [.NET Framework](https://dotnet.microsoft.com/learn/dotnet/what-is-dotnet-framework). Please migrate to **[.NET 8 SDK](https://dotnet.microsoft.com/en-us/download/dotnet/8.0)** instead.
  - `Microsoft.Spark.Sql.Streaming.DataStreamWriter.Foreach` does not work in .NET Framework ([#576](https://github.com/dotnet/spark/issues/576))
- `Microsoft.Spark.Worker` should be upgraded to 1.0 as `Microsoft.Spark.Worker` 0.x is not forward-compatible with `Microsoft.Spark` 1.0.
- `Microsoft.Spark` should be upgraded to 1.0 as `Microsoft.Spark.Worker` 1.0 is not backward-compatible with `Microsoft.Spark` 0.x.
- `Microsoft.Spark.Experimental` project has been merged into `Microsoft.Spark`
  - `VectorUdf` from `Microsoft.Spark.Sql.ExperimentalFunctions` is now part of `Microsoft.Spark.Sql.ArrowFunctions`.
  - `VectorUdf` from `Microsoft.Spark.Sql.ExperimentalDataFrameFunctions` is now part of `Microsoft.Spark.Sql.DataFrameFunctions`.
  - Extension methods have been moved from `Microsoft.Spark.Sql.RelationalGroupedDatasetExtensions` into the `Microsoft.Spark.Sql.RelationalGroupedDataset` class.
- Jar name has changed. ([#293](https://github.com/dotnet/spark/issues/293))([#728](https://github.com/dotnet/spark/issues/728))

  Old JAR  | New JAR
  ---------|---------
  microsoft-spark-2.3.x-`<version>`.jar | microsoft-spark-2-3_2.11-1.0.0.jar
  microsoft-spark-2.4.x-`<version>`.jar | microsoft-spark-2-4_2.11-1.0.0.jar
  microsoft-spark-3.0.x-`<version>`.jar | microsoft-spark-3-0_2.12-1.0.0.jar
