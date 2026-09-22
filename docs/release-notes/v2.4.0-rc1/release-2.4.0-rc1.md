# .NET for Apache Spark 2.4.0-rc1 Release Notes

This release candidate adds Apache Spark 4.0 integration and removes Spark 2.x support. `2.4.0-rc1` is the .NET for Apache Spark package version, not the Apache Spark runtime version.

The release branch and version metadata are prepared. Signed release artifacts and release-package validation are still pending; these notes do not announce package availability or a stable release.

## New Features and Improvements

- Add a Scala 2.13 bridge for Spark 4.0.0 through 4.0.4, built with JDK 17 ([#1249](https://github.com/dotnet/spark/pull/1249)).
- Support Spark 4 RDD execution, scalar .NET UDFs, Linux daemon workers and worker reuse ([#1249](https://github.com/dotnet/spark/pull/1249), [#1250](https://github.com/dotnet/spark/pull/1250), [#1251](https://github.com/dotnet/spark/pull/1251)).
- Add Spark 4 DataFrame row retrieval and broadcast support ([#1252](https://github.com/dotnet/spark/pull/1252), [#1255](https://github.com/dotnet/spark/pull/1255)).
- Support scalar Arrow UDFs and grouped-map UDFs on Spark 4 ([#1256](https://github.com/dotnet/spark/pull/1256)).
- Add Spark 4 Avro and Structured Streaming support, plus compatibility and persistence coverage for the existing ML wrappers ([#1297](https://github.com/dotnet/spark/pull/1297), [#1298](https://github.com/dotnet/spark/pull/1298), [#1299](https://github.com/dotnet/spark/pull/1299)).
- Add Spark 4 Driver API coverage and .NET Interactive scalar UDF support across cells, including recovery after failed submissions ([#1300](https://github.com/dotnet/spark/pull/1300)).
- Recognize `VariantType` in schemas and use Variant through Spark SQL expressions ([#1303](https://github.com/dotnet/spark/pull/1303)). See the [Variant guide](../../variant-guide.md) for examples and limitations.

## Bug Fixes

- Correct encrypted broadcast handling, including Spark 3.x behavior, and add Spark 4 broadcast protocol support ([#1255](https://github.com/dotnet/spark/pull/1255)).

## Build and Validation

- Expand the source E2E matrix to Spark 4.0.0 through 4.0.4 on Windows and Linux ([#1304](https://github.com/dotnet/spark/pull/1304)).
- Include the separately built Spark 4 bridge in release packaging, validate its packaged contents, and add Windows/Linux smoke tests using the final NuGet package and Worker archives ([#1305](https://github.com/dotnet/spark/pull/1305)).

These are validation capabilities in the source tree, not a record of successful RC artifact validation. Maintainers should follow the [release checklist](../../release-guide.md).

## Breaking Changes

- Remove Apache Spark 2.x and Scala 2.11 bridge support. Applications that must remain on Spark 2.x should keep their existing compatible library, Worker and bridge release together.
- Remove `Window.RangeBetween(Column, Column)`, `WindowSpec.RangeBetween(Column, Column)`, `Functions.UnboundedPreceding()`, `Functions.UnboundedFollowing()` and `Functions.CurrentRow()`. Use the `long`-based window APIs described in the [migration guide](../../migration-guide.md#removed-window-boundary-apis).
- Retire the experimental Spark 2.4/HDInsight notebook installer. This does not provide a replacement notebook deployment for Spark 4.

See [#1294](https://github.com/dotnet/spark/pull/1294) and the [migration guide](../../migration-guide.md) before upgrading.

## Runtime and Package Compatibility

Use the `Microsoft.Spark` NuGet package, its included bridge JAR and a Worker from the same candidate build. Spark 4 requires the new library and Worker; replacing only the JAR in a 2.3.1 deployment is not sufficient. Mixed-version Worker compatibility has not been established for this RC.

The following versions are accepted by the bridges in this branch. Acceptance by the version check is not a claim that every patch and feature has passed release validation.

| Apache Spark runtime | Bridge JAR for this RC |
| --- | --- |
| 3.0.0 through 3.0.2 | `microsoft-spark-3-0_2.12-2.4.0-rc1.jar` |
| 3.1.1, 3.1.2 | `microsoft-spark-3-1_2.12-2.4.0-rc1.jar` |
| 3.2.0 through 3.2.3 | `microsoft-spark-3-2_2.12-2.4.0-rc1.jar` |
| 3.3.0 through 3.3.4 | `microsoft-spark-3-3_2.12-2.4.0-rc1.jar` |
| 3.4.0 through 3.4.4 | `microsoft-spark-3-4_2.12-2.4.0-rc1.jar` |
| 3.5.0 through 3.5.3 | `microsoft-spark-3-5_2.12-2.4.0-rc1.jar` |
| 4.0.0 through 4.0.4 | `microsoft-spark-4-0_2.13-2.4.0-rc1.jar` |

Spark 4 candidate validation targets Windows and Linux x64 with .NET 8, Scala 2.13 and JDK 17. The packaged-artifact smoke test targets Spark 4.0.4. It does not establish Spark 4 compatibility for macOS, .NET Framework, managed services, Delta or Hyperspace. Existing Spark 3 extension compatibility should not be interpreted as Spark 4 extension support.

## Known Issues and Limitations

- Windows Spark 4 E2E runs have experienced hangs/timeouts. Root-cause investigation remains open, and this RC does not include a confirmed fix. A successful retry does not establish that the issue is resolved.
- Variant support is limited to schemas and Spark SQL. There is no supported native .NET Variant value or Variant UDF/Arrow input/output. Convert Variant columns to supported SQL types before retrieving rows into .NET.
- Spark 4 named UDF arguments, UDF profiling, Arrow UDFs defined in the REPL, and Arrow large variable-width types are not supported.
- Support for the existing API surface does not imply complete parity with every Spark 4 API, ML algorithm, extension or deployment environment.

Follow the [candidate setup instructions](../../migration-guide.md#trying-spark-40-from-source-or-a-candidate-package) to build from source or test matching candidate artifacts when they are available.
