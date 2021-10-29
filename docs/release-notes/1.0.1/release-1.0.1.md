# .NET for Apache Spark 1.0.1 Release Notes
This is a maintenance release which contains backported PRs to support Spark 2.4.8.

### New Features/Improvements

* Add spark.dotnet.ignoreSparkPatchVersionCheck conf to ignore patch version in DotnetRunner (#990)

### Bug Fixes

* Fix signer information mismatch issue (#989)

### Infrastructure / Documentation / Etc.

* Update dotnet-interactive deprecated feed (#991, #995)


### Breaking Changes

* None

### Known Issues

* Broadcast variables do not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#561](https://github.com/dotnet/spark/pull/561))
* UDFs defined using class objects with closures does not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#619](https://github.com/dotnet/spark/pull/619))
* In [dotnet-interactive](https://github.com/dotnet/interactive) blocking Spark methods that require external threads to unblock them does not work. ie `StreamingQuery.AwaitTermination` requires `StreamingQuery.Stop` to unblock ([#736](https://github.com/dotnet/spark/pull/736))
* `GroupedMap` does not work on Spark 3.0.0 ([#654](https://github.com/dotnet/spark/issues/654))

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
            <td>2.4.8</td>
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
            <td rowspan=8>1.0.1</td>
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
            <td rowspan=2>1.0.1</td>
        </tr>
        <tr>
            <td>0.2.0</td>
        </tr>
    </tbody>
</table>
