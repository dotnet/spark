# Preparing a Release

This guide is for maintainers preparing .NET for Apache Spark `2.4.0-rc1` from `release/branch-2.4.0-rc1`. The branch contains the version changes; producing and validating signed packages, creating a tag, and publishing a prerelease are separate steps. See the [candidate release notes](release-notes/v2.4.0-rc1/release-2.4.0-rc1.md) for user-facing changes and limitations.

## Select the source and version

Use a clean checkout of the release branch and record the exact commit being built. Do not include experimental debug changes without a separate review.

| Version source | RC value |
| --- | --- |
| `eng/Versions.props`: `VersionPrefix` / `VersionSuffix` | `2.4.0` / `rc1` |
| `src/scala/pom.xml`: `microsoft-spark.version` | `2.4.0-rc1` |
| `benchmark/scala/pom.xml`: project version | `2.4.0-rc1` |
| Release pipeline variable `DotnetPackageVersion` | `2.4.0-rc1` |

Keep `VersionPrefix` numeric; put `rc1` in `VersionSuffix`. The Maven parent supplies the version for all seven bridge modules, including the separately built Spark 4 module. Do not change Apache Spark dependency versions to the .NET package version.

`DotnetPackageVersion` is supplied by the pipeline configuration or when queueing a run, not declared in [azure-pipelines-release.yml](../azure-pipelines-release.yml). Set it explicitly to `2.4.0-rc1`. That pipeline passes it as `/p:Version` to the .NET build and uses it in Worker archive names. Arcade can derive build-specific versions, so editing the source version files alone is not proof of the final package version.

## Build and validate the candidate

1. Run the [source CI matrix](../azure-pipelines-pr.yml) against the selected release commit. Retain Windows and Linux coverage for Spark 3.x and Spark 4.0.0 through 4.0.4. Record failures, skips and retries, including the unresolved Windows Spark 4 timeout issue.
2. Queue the pipeline configured with [azure-pipelines-release.yml](../azure-pipelines-release.yml) on `release/branch-2.4.0-rc1`, with `DotnetPackageVersion=2.4.0-rc1`. The checked-in push triggers include only `main`; pushing the release branch does not automatically run these validations. The official pipeline needs its existing signing and package-source permissions.
3. Confirm that the Spark 3 reactor and benchmark build, and the separate JDK 17 Spark 4 bridge build, complete before the .NET packaging step. Use the [Windows](building/windows-instructions.md) or [Ubuntu](building/ubuntu-instructions.md) guide for local prerequisites.
4. Inspect the final `DotnetSpark` build artifact. Verify package manifests, bridge contents, Worker metadata and signatures as described below. Do not rely on filenames or a green signing summary alone.
5. Require both jobs in `ReleasePackageSmoke`, plus `ValidateWindows` and `ValidateLinux`, to succeed. Record any exception and the release owner's decision explicitly. The smoke jobs use final artifacts with Spark 4.0.4 and check Driver execution, `Collect` results and a scalar .NET UDF. They do not replace the full source E2E matrix or customer workload validation.

The release YAML publishes a build artifact. It does not publish a GitHub release or push packages to nuget.org.

## Inspect the final artifacts

The core package should be `Microsoft.Spark.2.4.0-rc1.nupkg`, with the same version in its `.nuspec`. Check any extension packages selected for publication independently. The core package should contain six Scala 2.12 Spark 3 bridges and `microsoft-spark-4-0_2.13-2.4.0-rc1.jar`, with no Spark 2.x bridge.

The release pipeline currently produces these Worker archives:

| Target | Archive |
| --- | --- |
| .NET 8, Windows x64 | `Microsoft.Spark.Worker.net8.0.win-x64-2.4.0-rc1.zip` |
| .NET 8, Linux x64 | `Microsoft.Spark.Worker.net8.0.linux-x64-2.4.0-rc1.zip` and `.tar.gz` |
| .NET 8, macOS x64 | `Microsoft.Spark.Worker.net8.0.osx-x64-2.4.0-rc1.zip` |
| .NET Framework 4.8, Windows x64 | `Microsoft.Spark.Worker.net48.win-x64-2.4.0-rc1.zip` |

Each archive has a `Microsoft.Spark.Worker-2.4.0-rc1/` root directory. A published archive does not by itself establish Spark 4 support for that platform; the Spark 4 smoke jobs exercise only the Windows and Linux .NET 8 ZIPs. Although the build also publishes a `net472` Worker directory, the release YAML does not archive or copy it into the release artifact.

Before public publication, resolve or explicitly review these validation gaps:

- The Worker subprocess in [eng/AfterSolutionBuild.targets](../eng/AfterSolutionBuild.targets) forwards `OfficialBuildId` and an explicitly supplied `/p:Version`. Inspect the extracted Worker DLL's informational/product version and the accompanying runtime metadata against the selected source commit and package version. Archive names alone do not prove the binaries have the intended RC version. Resolve unexpected version stamping before publication; renaming an archive is not a fix. Numeric assembly versions need not contain the prerelease suffix.
- Worker archives can be nested under runtime-specific directories in `DotnetSpark`. Existing signature checks use artifact-root globs for Worker archives and can miss those files. Inventory the archives recursively, verify the intended package and binary signatures, and retain the checked file list and verification results.
- Existing signature summaries can accept signature presence without full certificate trust validation. Resolve unsigned, unknown or untrusted results before treating signing as verified.

The [package validator](../eng/Test-SparkReleasePackage.ps1) checks the core NuGet identity, Spark 4 bridge structure and, when supplied, the bridge's SHA-256 against a freshly built JAR. It does not replace checking all artifact versions and signatures.

## Repeat the packaged smoke test locally

Use PowerShell 7, .NET 8 SDK, JDK 17 and Spark 4.0.4 on Windows or Linux x64. Windows also needs the [Hadoop tools prerequisites](building/windows-instructions.md#pre-requisites). Set `JAVA_HOME` to JDK 17 and use a new work directory for each run.

From the repository root, run the following with paths to the downloaded candidate artifacts, the matching freshly built JAR and the installed Spark distribution:

```powershell
./eng/Test-SparkReleasePackage.ps1 `
    -PackagePath '<release-artifacts>/Microsoft.Spark.2.4.0-rc1.nupkg' `
    -SparkJarPath 'src/scala/microsoft-spark-4-0/target/microsoft-spark-4-0_2.13-2.4.0-rc1.jar'

./eng/Run-SparkReleaseSmokeTest.ps1 `
    -PackageDirectory '<release-artifacts>' `
    -SparkHome '<spark-4.0.4-bin-hadoop3>' `
    -SparkVersion '4.0.4' `
    -WorkDirectory '<new-work-directory>'
```

If the original freshly built JAR is unavailable, omit `-SparkJarPath`; this skips the comparison with the build output, not the package structure checks. Do not substitute a JAR from another build and call it an identity check of the original output.

The artifact directory must contain exactly one core `Microsoft.Spark` NuGet package and one matching .NET 8 Worker ZIP for the current platform. Both are discovered recursively. The script restores a fresh test application from the candidate package, uses its embedded JAR and the extracted Worker, and retains logs in the work directory. Success includes `SPARK_RELEASE_SMOKE_TEST_PASSED`. Local unsigned tests do not verify official signatures, and smoke tests do not check every advertised API.

## Publish only after validation review

Before creating `v2.4.0-rc1` or publishing packages:

- Record the source commit, build identifier, artifact hashes, version/signature checks and test results for the exact candidate being published.
- Resolve release-blocking failures. Record the release owner's disposition of the Windows Spark 4 timeout issue and retain it in the known-issues section if unresolved; do not turn a successful retry into a fix claim.
- Update the candidate notes with the actual validation status and confirmed download instructions. Do not copy older releases' mixed-version Worker compatibility claims without new evidence.
- Create a GitHub **prerelease**, not a stable release, using the reviewed artifacts and notes. Public package publication is a separate authorized action; do not replace the signed and tested artifacts with a later local rebuild.

Creating the branch, changing version files or passing helper-script tests does not complete this checklist.
