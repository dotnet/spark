# Preparing a Release

This guide is for maintainers preparing the stable .NET for Apache Spark `2.4.0` release from `release/branch-2.4.0`. Preparing the branch, validating signed packages and publishing the release are separate steps. See the [2.4.0 release notes](release-notes/2.4.0/release-2.4.0.md) for user-facing changes, validation status and limitations.

## Select the source and version

Use a clean checkout of the release branch and record the exact commit being built. Do not include experimental debug changes without a separate review.

Record the final release commit after the version and documentation changes are committed. Build the GA packages from that exact commit. The branch includes the MessagePack update to `3.1.8` ([#1280](https://github.com/dotnet/spark/pull/1280)), so the RC1 artifact results do not validate the GA dependency set. Do not rename or republish RC1 artifacts as `2.4.0`.

| Version source | GA value |
| --- | --- |
| `eng/Versions.props`: `VersionPrefix` / `VersionSuffix` | `2.4.0` / empty |
| `src/scala/pom.xml`: `microsoft-spark.version` | `2.4.0` |
| `benchmark/scala/pom.xml`: project version | `2.4.0` |
| Release pipeline parameter `dotnetPackageVersion` | `2.4.0` |

Keep `VersionPrefix` numeric and omit or clear `VersionSuffix` for GA. The Maven parent supplies the version for all seven bridge modules, including the separately built Spark 4 module. Do not change Apache Spark dependency versions to the .NET package version.

In the [release pipeline](../azure-pipelines-release.yml) **Run pipeline** dialog, select `release/branch-2.4.0` and set the runtime parameter `dotnetPackageVersion` to `2.4.0`, its checked-in default. The pipeline binds that parameter to `DotnetPackageVersion`, passes it as `/p:Version` to the .NET build and uses it in Worker archive names. Arcade can derive build-specific versions, so editing the source version files alone is not proof of the final package version.

## Build and validate the candidate

1. Run the [source CI matrix](../azure-pipelines-pr.yml) against the selected release commit. Retain Windows and Linux coverage for Spark 3.x and Spark 4.0.0 through 4.0.4. Record failures, skips and retries, including the unresolved Windows Spark 4 timeout issue.
2. Queue the pipeline configured with [azure-pipelines-release.yml](../azure-pipelines-release.yml) on `release/branch-2.4.0`, with runtime parameter `dotnetPackageVersion=2.4.0`. The checked-in push triggers include only `main`; pushing the release branch does not automatically run these validations. The official pipeline needs its existing signing and package-source permissions. Confirm that the run uses the recorded release commit.
3. Confirm that the Spark 3 reactor and benchmark build, and the separate JDK 17 Spark 4 bridge build, complete before the .NET packaging step. Use the [Windows](building/windows-instructions.md) or [Ubuntu](building/ubuntu-instructions.md) guide for local prerequisites.
4. Inspect the final `DotnetSpark` build artifact. Verify package manifests, bridge contents, Worker metadata and signatures as described below. Do not rely on filenames or a green signing summary alone.
5. Require all 14 jobs in `ReleasePackageSmoke`, plus `ValidateWindows` and `ValidateLinux`, to succeed. The configured smoke matrix uses Spark 3.0.2, 3.1.2, 3.2.3, 3.3.4, 3.4.4, 3.5.3 and 4.0.4 on both Windows and Linux. Each job uses final artifacts and checks Driver execution, `Collect` results and a scalar .NET UDF. Record the actual results and any exception with the release owner's decision explicitly. These representative patches do not replace the full source E2E matrix or customer workload validation.

The release YAML publishes a build artifact. It does not publish a GitHub release or push packages to nuget.org.

## Inspect the final artifacts

The core package should be `Microsoft.Spark.2.4.0.nupkg`, with the same version in its `.nuspec` and a MessagePack dependency on `3.1.8`. Check any extension packages selected for publication independently. The core package should contain six Scala 2.12 Spark 3 bridges and `microsoft-spark-4-0_2.13-2.4.0.jar`, with no Spark 2.x bridge. Check all seven JAR versions and compare their SHA-256 hashes with the freshly built bridges from the recorded release commit.

The release pipeline currently produces these Worker archives:

| Target | Archive |
| --- | --- |
| .NET 8, Windows x64 | `Microsoft.Spark.Worker.net8.0.win-x64-2.4.0.zip` |
| .NET 8, Linux x64 | `Microsoft.Spark.Worker.net8.0.linux-x64-2.4.0.zip` and `.tar.gz` |
| .NET 8, macOS x64 | `Microsoft.Spark.Worker.net8.0.osx-x64-2.4.0.zip` |
| .NET Framework 4.8, Windows x64 | `Microsoft.Spark.Worker.net48.win-x64-2.4.0.zip` |

Each archive should have a `Microsoft.Spark.Worker-2.4.0/` root directory. A published archive does not by itself establish Spark support for that platform; the packaged smoke jobs exercise only the Windows and Linux .NET 8 ZIPs. Although the build also publishes a `net472` Worker directory, the release YAML does not archive or copy it into the release artifact.

For each candidate, perform these checks in addition to automated validation:

- The Worker subprocess in [eng/AfterSolutionBuild.targets](../eng/AfterSolutionBuild.targets) forwards `OfficialBuildId` and an explicitly supplied `/p:Version`. Inspect the extracted Worker DLL's informational/product version and the accompanying runtime metadata against the selected source commit and package version. Archive names alone do not prove the binaries have the intended GA version. Resolve unexpected version stamping before publication; renaming an archive is not a fix. Numeric assembly versions may differ from the package version.
- Inventory Worker archives recursively under `DotnetSpark`, including runtime-specific directories. Verify every intended NuGet package signature and project DLL/EXE signature, and retain the checked file list and verification results. The JARs and Worker archives do not have independent archive signatures; retain their hashes to identify the reviewed files.
- Require certificate trust validation as well as signature presence. Resolve unsigned, unknown or untrusted results before treating signing as verified. A green pipeline summary alone does not establish that every nested Worker binary was checked or trusted.

The [package validator](../eng/Test-SparkReleasePackage.ps1) checks the core NuGet identity, the selected Spark 3 or Spark 4 bridge structure and, when supplied, the bridge's SHA-256 against a freshly built JAR. It does not replace checking all artifact versions and signatures.

## Repeat the packaged smoke test locally

Use PowerShell 7, .NET 8 SDK and the selected Spark distribution on Windows or Linux x64. Set `JAVA_HOME` to JDK 8 for Spark 3 or JDK 17 for Spark 4. Windows also needs the [Hadoop tools prerequisites](building/windows-instructions.md#pre-requisites). Use a new work directory for each version and platform.

From the repository root, run the following Spark 4.0.4 example with paths to the downloaded candidate artifacts, the matching freshly built JAR and the installed Spark distribution. For Spark 3, select the corresponding `-SparkVersion` and `-SparkHome`, and pass the matching `-SparkMajorMinorVersion` (for example, `3.5`) to the package validator:

```powershell
./eng/Test-SparkReleasePackage.ps1 `
    -PackagePath '<release-artifacts>/Microsoft.Spark.2.4.0.nupkg' `
    -SparkJarPath 'src/scala/microsoft-spark-4-0/target/microsoft-spark-4-0_2.13-2.4.0.jar'

./eng/Run-SparkReleaseSmokeTest.ps1 `
    -PackageDirectory '<release-artifacts>' `
    -SparkHome '<spark-4.0.4-bin-hadoop3>' `
    -SparkVersion '4.0.4' `
    -WorkDirectory '<new-work-directory>'
```

If the original freshly built JAR is unavailable, omit `-SparkJarPath`; this skips the comparison with the build output, not the package structure checks. Do not substitute a JAR from another build and call it an identity check of the original output.

The artifact directory must contain exactly one core `Microsoft.Spark` NuGet package and one matching .NET 8 Worker ZIP for the current platform. Both are discovered recursively. The script restores a fresh test application from the candidate package, uses its embedded JAR and the extracted Worker, and retains logs in the work directory. Success includes `SPARK_RELEASE_SMOKE_TEST_PASSED`. Local unsigned tests do not verify official signatures, and smoke tests do not check every advertised API.

## Publish only after validation review

Before creating `v2.4.0` or publishing packages:

- Record the source commit, build identifier, artifact hashes, version/signature checks and test results for the exact candidate being published.
- Resolve release-blocking failures. Record the release owner's disposition of the Windows Spark 4 timeout issue and retain it in the known-issues section if unresolved; do not turn a successful retry into a fix claim.
- Update the [2.4.0 notes](release-notes/2.4.0/release-2.4.0.md) with the actual GA validation status, exact source commit, build identifier, artifact hashes and confirmed download instructions. RC1 and PR source-CI results remain historical evidence for the commits and artifacts tested. Do not copy older releases' mixed-version Worker compatibility claims without new evidence.
- Tag the recorded release commit as `v2.4.0` and create a stable GitHub release with `prerelease=false`, using the reviewed artifacts and notes. Publish the selected signed NuGet packages through the approved package-publication process. Preserve the existing `v2.4.0-rc1` tag and assets, and do not replace the signed and tested GA artifacts with a later local rebuild.

Creating the branch, changing version files or passing helper-script tests does not complete this checklist.

## RC1 validation history

The published `2.4.0-rc1` artifacts were built from commit `46944bfd0cb1bdf01ec30a79b6b57eb803f3a62c`. The release tag `v2.4.0-rc1` points to `6e1ce99c6de4238ffc84ea4bb06db41f6c9e0856`, which additionally includes documentation-only updates. Runtime code, build configuration and dependency declarations are unchanged between those two RC commits. See the unchanged [RC1 notes](release-notes/v2.4.0-rc1/release-2.4.0-rc1.md) for their validation results. This provenance applies to RC1, not to the GA build with MessagePack `3.1.8`.
