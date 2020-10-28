# .NET for Apache Spark 1.0.1 Release Notes

### New Features/Improvements

* Use pattern matching in arrow test utils to improve readability ([#725](https://github.com/dotnet/spark/pull/725))
* Support for Arrow 2.0 and GroupedMapUdf in Spark 3.0.0 ([#654](https://github.com/dotnet/spark/issues/654))

### Bug Fixes

* Fix signer information mismatch issue ([#752](https://github.com/dotnet/spark/pull/752))

### Infrastructure / Documentation / Etc.

* Fix flaky CallbackTests.TestCallbackHandlers Test ([#745](https://github.com/dotnet/spark/pull/745))

### Breaking Changes

* None

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
            <td rowspan>v1.0.0</td>
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
            <td>microsoft-spark-2-3_2.11-1.0.1.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=7>microsoft-spark-2-4_2.11-1.0.1.jar</td>
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
            <td rowspan=2>microsoft-spark-3-0_2.12-1.0.1.jar</td>
        </tr>
        <tr>
            <td>3.0.1</td>
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
            <td rowspan=8>1.0.0</td>
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
            <td rowspan=2>1.0.0</td>
        </tr>
        <tr>
            <td>0.2.0</td>
        </tr>
    </tbody>
</table>
