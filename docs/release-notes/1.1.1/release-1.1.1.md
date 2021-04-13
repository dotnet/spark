# .NET for Apache Spark 1.1.1 Release Notes

* Note that there was a breaking forward compatibility change ([#871](https://github.com/dotnet/spark/pull/871)) that went into the 1.1.0 release. This is the re-release of 1.1.0 after reverting the change. If you are using the 1.1.0 release, it is strongly encouraged to upgrade to 1.1.1.

### Deprecation notice for Spark 2.3

We are planning to drop the support for Spark 2.3 in the 2.0 release, which will be the next release.
The last Spark 2.3 release (2.3.4) was back in September 2019, and no new release is planned for Spark 2.3. Since there have been no new features introduced for Spark 2.3 in the last few releases of .NET for Apache Spark, if you are relying on Spark 2.3, you should be able to continue using .NET for Apache Spark 1.x.

### New Features/Improvements

* Support for Arrow 2.0 and GroupedMapUdf in Spark 3.0.0 ([#711](https://github.com/dotnet/spark/pull/711))
* Use pattern matching in arrow test utils to improve readability ([#725](https://github.com/dotnet/spark/pull/725))
* Check whether file is found before trying to dereference it ([#759](https://github.com/dotnet/spark/pull/759))
* Ml/feature hasher has only internal contructors ([#761](https://github.com/dotnet/spark/pull/761))
* Support for stop words removers ([#726](https://github.com/dotnet/spark/pull/726))
* Support for adding NGram functionality ([#734](https://github.com/dotnet/spark/pull/734))
* Add support for SQLTransformer ML feature ([#781](https://github.com/dotnet/spark/pull/781))
* Add skeletal support for FileSystem extension ([#787](https://github.com/dotnet/spark/pull/787))
* Using (processId, threadId) as key to mantain threadpool executor instead of only threadId ([#793](https://github.com/dotnet/spark/pull/793))
* Support for Hyperspace 0.4.0 ([#815](https://github.com/dotnet/spark/pull/815))
* Support for Delta Lake 0.8.0 ([#823](https://github.com/dotnet/spark/pull/823))
* Add support for Spark 3.0.2 ([#833](https://github.com/dotnet/spark/pull/833))
* Add DOTNET_WORKER_<ver>_DIR environment variable ([#861](https://github.com/dotnet/spark/pull/861))
* Add spark.dotnet.ignoreSparkPatchVersionCheck conf to ignore patch version in DotnetRunner ([#862](https://github.com/dotnet/spark/pull/862))

### Bug Fixes

* Fix signer information mismatch issue ([#752](https://github.com/dotnet/spark/pull/752))
* Fix package-worker.ps1 to handle output path with ":" ([#742](https://github.com/dotnet/spark/pull/742))
* Fix for using Broadcast variables in Databricks ([#766](https://github.com/dotnet/spark/pull/766))
* Fix macOS Catalina Permissions ([#784](https://github.com/dotnet/spark/pull/784))
* Fix for memory leak in JVMObjectTracker ([#801](https://github.com/dotnet/spark/pull/801))
* Bug Fix for Spark 3.x - Avoid converting converted Row values ([#868](https://github.com/dotnet/spark/pull/868))
* Add 'Z' to the string format in Timestamp.ToString() to indicate UTC time ([#871](https://github.com/dotnet/spark/pull/871))

### Infrastructure / Documentation / Etc.

* Fix flaky CallbackTests.TestCallbackHandlers Test ([#745](https://github.com/dotnet/spark/pull/745))
* Run E2E tests on Linux in build pipeline and add Backward/Forward E2E tests ([#737](https://github.com/dotnet/spark/pull/737))
* Update dotnet-interactive deprecated feed ([#807](https://github.com/dotnet/spark/pull/807), [#808](https://github.com/dotnet/spark/pull/808))
* Remove unnecessary RestoreSources ([#812](https://github.com/dotnet/spark/pull/812))
* Migrating master to main branch ([#847](https://github.com/dotnet/spark/pull/847), [#849](https://github.com/dotnet/spark/pull/849))

### Breaking Changes

* None

### Known Issues

* Broadcast variables do not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#561](https://github.com/dotnet/spark/pull/561))
* UDFs defined using class objects with closures does not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#619](https://github.com/dotnet/spark/pull/619))
* In [dotnet-interactive](https://github.com/dotnet/interactive) blocking Spark methods that require external threads to unblock them does not work. ie `StreamingQuery.AwaitTermination` requires `StreamingQuery.Stop` to unblock ([#736](https://github.com/dotnet/spark/pull/736))
* UDFs don't work in Linux with Spark 2.3.0 ([#753](https://github.com/dotnet/spark/issues/753))

### Compatibility

#### Backward compatibility

The following table describes the oldest version of the worker that the current version is compatible with, along with new features that are incompatible with the worker.

<table>
    <thead>
        <tr>
            <th>Oldest compatible Microsoft.Spark.Worker version</th>
            <th>Incompatible features</th>
        </tr>
    </thead>
    <tbody align="center">
        <tr>
            <td>v1.0.0</td>
            <td>GroupedMap in Spark 3.0 is not compatible with Worker 1.0 <a href="https://github.com/dotnet/spark/pull/654">(#654)</a>*</td>
        </tr>
    </tbody>
</table>
* This is not a breaking change since this feature never worked with Worker 1.0.0.

#### Forward compatibility

The following table describes the oldest version of .NET for Apache Spark release that the current worker is compatible with.

<table>
    <thead>
        <tr>
            <th>Oldest compatible .NET for Apache Spark release version</th>
        </tr>
    </thead>
    <tbody align="center">
        <tr>
            <td>v1.0.0</td>
        </tr>
    </tbody>
</table>

### Supported Spark Versions

The following table outlines the supported Spark versions along with the microsoft-spark JAR to use with:

<table>
    <thead>
        <tr>
            <th>Spark Version</th>
            <th>microsoft-spark JAR</th>
        </tr>
    </thead>
    <tbody align="center">
        <tr>
            <td>2.3.*</td>
            <td>microsoft-spark-2-3_2.11-1.1.1.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=7>microsoft-spark-2-4_2.11-1.1.1.jar</td>
        </tr>
        <tr>
            <td>2.4.1</td>
        </tr>
        <tr>
            <td>2.4.3</td>
        </tr>
        <tr>
            <td>2.4.4</td>
        </tr>
        <tr>
            <td>2.4.5</td>
        </tr>
        <tr>
            <td>2.4.6</td>
        </tr>
        <tr>
            <td>2.4.7</td>
        </tr>
        <tr>
            <td>2.4.2</td>
            <td><a href="https://github.com/dotnet/spark/issues/60">Not supported</a></td>
        </tr>
        <tr>
            <td>3.0.0</td>
            <td rowspan=3>microsoft-spark-3-0_2.12-1.1.1.jar</td>
        </tr>
        <tr>
            <td>3.0.1</td>
        </tr>
		<tr>
            <td>3.0.2</td>
        </tr>
    </tbody>
</table>

### Supported Delta Versions

The following table outlines the supported Delta versions along with the Microsoft.Spark.Extensions version to use with:

<table>
    <thead>
        <tr>
            <th>Delta Version</th>
            <th>Microsoft.Spark.Extensions.Delta</th>
        </tr>
    </thead>
    <tbody align="center">
        <tr>
            <td>0.1.0</td>
            <td rowspan=9>1.1.1</td>
        </tr>
        <tr>
            <td>0.2.0</td>
        </tr>
        <tr>
            <td>0.3.0</td>
        </tr>
        <tr>
            <td>0.4.0</td>
        </tr>
        <tr>
            <td>0.5.0</td>
        </tr>
        <tr>
            <td>0.6.0</td>
        </tr>
        <tr>
            <td>0.6.1</td>
        </tr>
        <tr>
            <td>0.7.0</td>
        </tr>
		<tr>
            <td>0.8.0</td>
        </tr>
    </tbody>
</table>

### Supported Hyperspace Versions

The following table outlines the supported Hyperspace versions along with the Microsoft.Spark.Extensions version to use with:

<table>
    <thead>
        <tr>
            <th>Hyperspace Version</th>
            <th>Microsoft.Spark.Extensions.Hyperspace</th>
        </tr>
    </thead>
    <tbody align="center">
        <tr>
            <td>0.1.0</td>
            <td rowspan=4>1.1.1</td>
        </tr>
        <tr>
            <td>0.2.0</td>
        </tr>
		<tr>
            <td>0.3.0</td>
        </tr>
		<tr>
            <td>0.4.0</td>
        </tr>
    </tbody>
</table>
