# .NET for Apache Spark 2.0.0 Release Notes

### Deprecation for Spark 2.3

We have officially dropped support for Spark 2.3 in the 2.0.0 release. The last Spark 2.3 release (2.3.4) was back in September 2019, and no new release is planned for Spark 2.3. Since there have been no new features introduced for Spark 2.3 in the last few releases of .NET for Apache Spark, if you are relying on Spark 2.3, you should be able to continue using .NET for Apache Spark 1.x.

### New Features/Improvements

* Support for Spark 3.1.0 APIs ([#886](https://github.com/dotnet/spark/pull/886), [#887](https://github.com/dotnet/spark/pull/887), [#888](https://github.com/dotnet/spark/pull/888), [#889](https://github.com/dotnet/spark/pull/889), [#890](https://github.com/dotnet/spark/pull/890), [#893](https://github.com/dotnet/spark/pull/893))
* Support for Spark 2.4.8, 3.1.2 ([#953](https://github.com/dotnet/spark/pull/953))
* Add `Exists(string path)` to `Microsoft.Spark.Hadoop.Fs.FileSystem` ([#909](https://github.com/dotnet/spark/pull/909))
* Add `Version()` to `Microsoft.Spark.SparkContext` and `Microsoft.Spark.Sql.SparkSession` ([#919](https://github.com/dotnet/spark/pull/919))
* Add Avro `ToAvro`, `FromAvro` APIs ([#805](https://github.com/dotnet/spark/pull/805))
* Extensions.DotNet.Interactive add ENV var to control disposal of tmp dir ([#952](https://github.com/dotnet/spark/pull/952))
* Add public APIs to access `JvmBridge`, `JvmObjectReference`, `IJvmObjectReferenceProvider` ([#951](https://github.com/dotnet/spark/pull/951))
  > This is exposed to help users interact with the JVM. It is provided with limited support and should be used with caution.

### Bug Fixes

* None

### Infrastructure / Documentation / Etc.

* None

### Breaking Changes

* Add 'Z' to the string format in Timestamp.ToString() to indicate UTC time ([#897](https://github.com/dotnet/spark/pull/897))

### Known Issues

* Broadcast variables do not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#561](https://github.com/dotnet/spark/pull/561))
* UDFs defined using class objects with closures does not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#619](https://github.com/dotnet/spark/pull/619))
* In [dotnet-interactive](https://github.com/dotnet/interactive) blocking Spark methods that require external threads to unblock them does not work. ie `StreamingQuery.AwaitTermination` requires `StreamingQuery.Stop` to unblock ([#736](https://github.com/dotnet/spark/pull/736))

### Compatibility

#### Backward compatibility

The following table describes the oldest version of the worker that the current version is compatible with, along with new features that are incompatible with the worker.

<table>
    <thead>
        <tr>
            <th>Oldest compatible Microsoft.Spark.Worker version</th>
        </tr>
    </thead>
    <tbody align="center">
        <tr>
            <td>v2.0.0</td>
        </tr>
    </tbody>
</table>

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
            <td>v2.0.0</td>
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
            <td>2.4.0</td>
            <td rowspan=7>microsoft-spark-2-4_2.11-2.0.0.jar</td>
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
            <td rowspan=3>microsoft-spark-3-0_2.12-2.0.0.jar</td>
        </tr>
        <tr>
            <td>3.0.1</td>
        </tr>
		<tr>
            <td>3.0.2</td>
        </tr>
        <tr>
            <td>3.1.1</td>
            <td rowspan=3>microsoft-spark-3-1_2.12-2.0.0.jar</td>
        </tr>
        <tr>
            <td>3.1.2</td>
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
            <td rowspan=9>2.0.0</td>
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
            <td rowspan=4>2.0.0</td>
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
