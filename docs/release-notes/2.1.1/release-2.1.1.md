# .NET for Apache Spark 2.1.1 Release Notes

### New Features/Improvements

* Add base classes for ML and refine code base ([#1031](https://github.com/dotnet/spark/pull/1031), [#1049](https://github.com/dotnet/spark/pull/1049))
* Capture dotnet application error stack trace ([#1047](https://github.com/dotnet/spark/pull/1047))
* JvmBridge semaphore fix ([#1061](https://github.com/dotnet/spark/pull/1061))

### Bug Fixes

* None

### Infrastructure / Documentation / Etc.

* Switch to 1ES hosted pools on main ([#1021](https://github.com/dotnet/spark/pull/1021))

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
            <td rowspan=7>microsoft-spark-2-4_2.11-2.1.1.jar</td>
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
            <td rowspan=3>microsoft-spark-3-0_2.12-2.1.1.jar</td>
        </tr>
        <tr>
            <td>3.0.1</td>
        </tr>
		<tr>
            <td>3.0.2</td>
        </tr>
        <tr>
            <td>3.1.1</td>
            <td rowspan=2>microsoft-spark-3-1_2.12-2.1.1.jar</td>
        </tr>
        <tr>
            <td>3.1.2</td>
        </tr>
        <tr>
            <td>3.2.0</td>
            <td rowspan=2>microsoft-spark-3-2_2.12-2.1.1.jar</td>
        </tr>
        <tr>
            <td>3.2.1</td>
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
            <td rowspan=11>2.1.1</td>
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
        <tr>
            <td>1.0.0</td>
        </tr>
		<tr>
            <td>1.1.0</td>
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
            <td rowspan=4>2.1.1</td>
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
