# .NET for Apache Spark 2.4.0-rc1 Release Notes

This release candidate adds Apache Spark 4.0 integration and removes Spark 2.x support. `2.4.0-rc1` is the .NET for Apache Spark package version, not the Apache Spark runtime version.

The signed candidate packages have completed release-package validation. This prerelease is intended for evaluation and feedback, not as a stable release. See [Getting the packages](#getting-the-packages) for installation and Worker downloads.

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

The candidate artifacts were built from commit [`46944bfd`](https://github.com/dotnet/spark/commit/46944bfd0cb1bdf01ec30a79b6b57eb803f3a62c) and validated as follows:

- Package identities, versions, all seven bridge JARs and Worker metadata were checked. All four NuGet package signatures and the intended project DLL/EXE signatures were verified. Bridge JARs and Worker archives do not have independent archive signatures.
- All 14 packaged-artifact smoke jobs passed on their first attempt: Spark 3.0.2, 3.1.2, 3.2.3, 3.3.4, 3.4.4, 3.5.3 and 4.0.4 on Windows and Linux. These tests exercised Driver execution, `Collect` and scalar .NET UDFs using the final packages. Windows and Linux package-installation validation also passed.
- The source E2E matrix completed successfully for the same commit. Windows Spark 4.0.0, 4.0.1 and 4.0.3 jobs required a retry; this is not evidence that the timeout issue is fixed.
- The existing core E2E suite was run locally on Windows x64 with .NET 8 against the final NuGet package, its bridge JARs and the packaged Worker. Spark 4.0.4 had 193 passed, 1 skipped and 0 failed tests; Spark 3.5.3 had 173 passed, 21 skipped and 0 failed tests. Skips followed the existing version gates, including the `AddArchive`/`ListArchives` signature test on both versions.

These results do not establish compatibility with every API or deployment environment. Maintainers should follow the [release checklist](../../release-guide.md), and users should validate their own workloads before upgrading.

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

Spark 4 candidate validation covers Windows and Linux x64 with .NET 8, Scala 2.13 and JDK 17. The packaged-artifact smoke results above cover one representative patch per Spark 3.0 through 3.5 minor and Spark 4.0.4 on both platforms. This does not establish Spark 4 compatibility for macOS, .NET Framework, managed services, Delta or Hyperspace. Existing Spark 3 extension compatibility should not be interpreted as Spark 4 extension support.

## Getting the packages

Install the [`Microsoft.Spark` 2.4.0-rc1 NuGet package](https://www.nuget.org/packages/Microsoft.Spark/2.4.0-rc1):

```shell
dotnet add package Microsoft.Spark --version 2.4.0-rc1
```

Download the matching `Microsoft.Spark.Worker` archive from the [GitHub prerelease](https://github.com/dotnet/spark/releases/tag/v2.4.0-rc1). Package and asset publication is separate from validation; these download links become available when the corresponding artifacts are published.

| Platform | .NET 8 | .NET Framework 4.8 |
| --- | --- | --- |
| Linux | x64 ([tar.gz](https://github.com/dotnet/spark/releases/download/v2.4.0-rc1/Microsoft.Spark.Worker.net8.0.linux-x64-2.4.0-rc1.tar.gz) \| [zip](https://github.com/dotnet/spark/releases/download/v2.4.0-rc1/Microsoft.Spark.Worker.net8.0.linux-x64-2.4.0-rc1.zip)) | N/A |
| Windows | x64 ([zip](https://github.com/dotnet/spark/releases/download/v2.4.0-rc1/Microsoft.Spark.Worker.net8.0.win-x64-2.4.0-rc1.zip)) | x64 ([zip](https://github.com/dotnet/spark/releases/download/v2.4.0-rc1/Microsoft.Spark.Worker.net48.win-x64-2.4.0-rc1.zip)) |
| macOS | x64 ([zip](https://github.com/dotnet/spark/releases/download/v2.4.0-rc1/Microsoft.Spark.Worker.net8.0.osx-x64-2.4.0-rc1.zip)) | N/A |

For Spark 4, choose the Windows or Linux x64 .NET 8 Worker. Extract the archive and set `DOTNET_WORKER_DIR` to its `Microsoft.Spark.Worker-2.4.0-rc1` directory. Use JDK 17, a Scala 2.13 Spark distribution and `microsoft-spark-4-0_2.13-2.4.0-rc1.jar` from the NuGet package. Use the matching Worker on every executor.

Follow the [candidate setup instructions](../../migration-guide.md#trying-spark-40-from-source-or-a-candidate-package) for environment configuration and deployment.

## Known Issues and Limitations

- Windows Spark 4 E2E runs have experienced hangs/timeouts. Root-cause investigation remains open, and this RC does not include a confirmed fix. A successful retry does not establish that the issue is resolved.
- Variant support is limited to schemas and Spark SQL. There is no supported native .NET Variant value or Variant UDF/Arrow input/output. Convert Variant columns to supported SQL types before retrieving rows into .NET.
- Spark 4 named UDF arguments, UDF profiling, Arrow UDFs defined in the REPL, and Arrow large variable-width types are not supported.
- Support for the existing API surface does not imply complete parity with every Spark 4 API, ML algorithm, extension or deployment environment.
