# .NET for Apache Spark 2.4.0 Release Notes

This release adds Apache Spark 4.0 integration, removes Spark 2.x support and updates MessagePack to `3.1.8`. `2.4.0` is the .NET for Apache Spark package version, not the Apache Spark runtime version.

The `release/branch-2.4.0` branch is prepared for the stable release. Validation of the signed GA packages and publication to NuGet and GitHub remain pending. The available [`2.4.0-rc1` prerelease](https://github.com/dotnet/spark/releases/tag/v2.4.0-rc1) has its own [validation results and download instructions](../v2.4.0-rc1/release-2.4.0-rc1.md); those results do not validate the new GA artifacts.

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

## Maintenance and Community Contributions

- Update MessagePack from `3.0.214-rc.1` to the stable `3.1.8` dependency ([#1280](https://github.com/dotnet/spark/pull/1280)), including the upstream fix for unbounded recursion in `MessagePackReader.Skip` ([GHSA-vh6j-jc39-fggf](https://github.com/advisories/GHSA-vh6j-jc39-fggf)).
- Declare the MIT license through NuGet's SPDX license-expression metadata, contributed by [@thompson-tomo](https://github.com/thompson-tomo) ([#1216](https://github.com/dotnet/spark/pull/1216)).
- Add automated .NET formatting checks for the library, examples and benchmarks, and apply formatting cleanup, contributed by [@sheddy123](https://github.com/sheddy123) ([#1246](https://github.com/dotnet/spark/pull/1246)).
- Simplify internal IPC type dispatch with a switch expression, contributed by [@aadvark-pancakes](https://github.com/aadvark-pancakes) ([#1239](https://github.com/dotnet/spark/pull/1239)).
- Remove an obsolete bot variable-group reference from the build pipeline, contributed by [@missymessa](https://github.com/missymessa) ([#1248](https://github.com/dotnet/spark/pull/1248)).
- Add Dependabot configuration for repository dependencies, contributed by [@aleks-ivanov](https://github.com/aleks-ivanov) ([#944](https://github.com/dotnet/spark/pull/944)).

## Build and Validation

- Expand the source E2E matrix to Spark 4.0.0 through 4.0.4 on Windows and Linux ([#1304](https://github.com/dotnet/spark/pull/1304)).
- Include the separately built Spark 4 bridge in release packaging, validate its packaged contents, and add Windows/Linux smoke tests using the final NuGet package and Worker archives ([#1305](https://github.com/dotnet/spark/pull/1305)).

The RC1 artifacts from commit [`46944bfd`](https://github.com/dotnet/spark/commit/46944bfd0cb1bdf01ec30a79b6b57eb803f3a62c) passed all 14 packaged-artifact smoke jobs, Windows/Linux package-installation validation and the documented version/signature checks. Their source E2E matrix completed successfully after retries in three Windows Spark 4 jobs. The [RC1 notes](../v2.4.0-rc1/release-2.4.0-rc1.md#build-and-validation) retain the complete results and limitations.

The MessagePack update in [#1280](https://github.com/dotnet/spark/pull/1280), merged as [`cb07b2e`](https://github.com/dotnet/spark/commit/cb07b2ea6e6d78b06b87c519eb10746bec7baa82), passed PR source-CI [build `74587971`](https://msasg.visualstudio.com/ManagedOSS/_build/results?buildId=74587971), including all 54 E2E jobs. This validates the PR source baseline; it does not validate the final GA release commit or signed `2.4.0` packages.

Before publication, maintainers must record the final GA source commit and official build, verify package/bridge/Worker versions and trusted signatures, and pass all 14 `ReleasePackageSmoke` jobs plus `ValidateWindows` and `ValidateLinux` against the exact signed GA artifacts. Artifact hashes, test results and the release owner's disposition of the Windows Spark 4 timeout issue must be recorded in these notes. Follow the [release checklist](../../release-guide.md) to complete that review.

## Breaking Changes

- Remove Apache Spark 2.x and Scala 2.11 bridge support. Applications that must remain on Spark 2.x should keep their existing compatible library, Worker and bridge release together.
- Remove `Window.RangeBetween(Column, Column)`, `WindowSpec.RangeBetween(Column, Column)`, `Functions.UnboundedPreceding()`, `Functions.UnboundedFollowing()` and `Functions.CurrentRow()`. Use the `long`-based window APIs described in the [migration guide](../../migration-guide.md#removed-window-boundary-apis).
- Retire the experimental Spark 2.4/HDInsight notebook installer. This does not provide a replacement notebook deployment for Spark 4.

See [#1294](https://github.com/dotnet/spark/pull/1294) and the [migration guide](../../migration-guide.md) before upgrading.

## Runtime and Package Compatibility

Use the `Microsoft.Spark` NuGet package, its included bridge JAR and a Worker from the same release build. Spark 4 requires the new library and Worker; replacing only the JAR in a 2.3.1 deployment is not sufficient. Mixed-version Worker compatibility has not been established for `2.4.0`.

The following versions are accepted by the bridges in this branch. Acceptance by the version check is not a claim that every patch and feature has passed GA artifact validation.

| Apache Spark runtime | Bridge JAR for 2.4.0 |
| --- | --- |
| 3.0.0 through 3.0.2 | `microsoft-spark-3-0_2.12-2.4.0.jar` |
| 3.1.1, 3.1.2 | `microsoft-spark-3-1_2.12-2.4.0.jar` |
| 3.2.0 through 3.2.3 | `microsoft-spark-3-2_2.12-2.4.0.jar` |
| 3.3.0 through 3.3.4 | `microsoft-spark-3-3_2.12-2.4.0.jar` |
| 3.4.0 through 3.4.4 | `microsoft-spark-3-4_2.12-2.4.0.jar` |
| 3.5.0 through 3.5.3 | `microsoft-spark-3-5_2.12-2.4.0.jar` |
| 4.0.0 through 4.0.4 | `microsoft-spark-4-0_2.13-2.4.0.jar` |

The Spark 4 validation target is Windows and Linux x64 with .NET 8, Scala 2.13 and JDK 17. The packaged-artifact smoke matrix covers one representative patch per Spark 3.0 through 3.5 minor and Spark 4.0.4 on both platforms. Publishing a macOS or .NET Framework Worker archive does not establish Spark 4 compatibility there, or for managed services, Delta or Hyperspace. Existing Spark 3 extension compatibility should not be interpreted as Spark 4 extension support.

## Getting the packages

Once the reviewed GA artifacts are published, install the [`Microsoft.Spark` 2.4.0 NuGet package](https://www.nuget.org/packages/Microsoft.Spark/2.4.0):

```shell
dotnet add package Microsoft.Spark --version 2.4.0
```

Download the matching `Microsoft.Spark.Worker` archive from the [`v2.4.0` GitHub release](https://github.com/dotnet/spark/releases/tag/v2.4.0). These links are the intended publication locations and do not confirm current package or asset availability.

| Platform | .NET 8 | .NET Framework 4.8 |
| --- | --- | --- |
| Linux | x64 ([tar.gz](https://github.com/dotnet/spark/releases/download/v2.4.0/Microsoft.Spark.Worker.net8.0.linux-x64-2.4.0.tar.gz) \| [zip](https://github.com/dotnet/spark/releases/download/v2.4.0/Microsoft.Spark.Worker.net8.0.linux-x64-2.4.0.zip)) | N/A |
| Windows | x64 ([zip](https://github.com/dotnet/spark/releases/download/v2.4.0/Microsoft.Spark.Worker.net8.0.win-x64-2.4.0.zip)) | x64 ([zip](https://github.com/dotnet/spark/releases/download/v2.4.0/Microsoft.Spark.Worker.net48.win-x64-2.4.0.zip)) |
| macOS | x64 ([zip](https://github.com/dotnet/spark/releases/download/v2.4.0/Microsoft.Spark.Worker.net8.0.osx-x64-2.4.0.zip)) | N/A |

For Spark 4, choose the Windows or Linux x64 .NET 8 Worker. Extract the archive and set `DOTNET_WORKER_DIR` to its `Microsoft.Spark.Worker-2.4.0` directory. Use JDK 17, a Scala 2.13 Spark distribution and `microsoft-spark-4-0_2.13-2.4.0.jar` from the NuGet package. Use the matching Worker on every executor.

Follow the [setup instructions](../../migration-guide.md#using-spark-40-with-net-for-apache-spark-240) for environment configuration and deployment. Until GA publication is complete, use the [RC1 package instructions](../v2.4.0-rc1/release-2.4.0-rc1.md#getting-the-packages) to evaluate the published candidate.

## Known Issues and Limitations

- Windows Spark 4 E2E runs have experienced hangs/timeouts. Root-cause investigation remains open, and `2.4.0` does not include a confirmed fix. A successful retry or CI run does not establish that the issue is resolved.
- Variant support is limited to schemas and Spark SQL. There is no supported native .NET Variant value or Variant UDF/Arrow input/output. Convert Variant columns to supported SQL types before retrieving rows into .NET.
- Spark 4 named UDF arguments, UDF profiling, Arrow UDFs defined in the REPL, and Arrow large variable-width types are not supported.
- Support for the existing API surface does not imply complete parity with every Spark 4 API, ML algorithm, extension or deployment environment. Validate your application's APIs and deployment before upgrading.
