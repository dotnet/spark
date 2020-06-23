# .NET for Apache Spark 0.12 Release Notes

### New Features/Improvements and Bug Fixes

* Expose `DataStreamWriter.ForeachBatch` API ([#549](https://github.com/dotnet/spark/pull/549))
* Support for [dotnet-interactive](https://github.com/dotnet/interactive) ([#515](https://github.com/dotnet/spark/pull/515)) ([#517](https://github.com/dotnet/spark/pull/517)) ([#554](https://github.com/dotnet/spark/pull/554))
* Support for [Hyperspace v0.1.0](https://github.com/microsoft/hyperspace) APIs ([#555](https://github.com/dotnet/spark/pull/555))
* Support for Spark 2.4.6 ([#547](https://github.com/dotnet/spark/pull/547))
* Bug fix caused by `BroadcastVariablesRegistry` ([#551](https://github.com/dotnet/spark/pull/551))
* Update `Microsoft.Data.Analysis` to v`0.4.0` ([#528](https://github.com/dotnet/spark/pull/528))

### Infrastructure / Documentation / Etc.

* Improve build pipeline ([#510](https://github.com/dotnet/spark/pull/510)) ([#511](https://github.com/dotnet/spark/pull/511)) ([#512](https://github.com/dotnet/spark/pull/512)) ([#513](https://github.com/dotnet/spark/pull/513)) ([#524](https://github.com/dotnet/spark/pull/524))
* Update AppName for the C# Spark Examples ([#548](https://github.com/dotnet/spark/pull/548))
* Update maven links in build documentation ([#558](https://github.com/dotnet/spark/pull/558)) ([#560](https://github.com/dotnet/spark/pull/560))

### Breaking Changes

* None

### Known Issues

* Broadcast variables do not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#561](https://github.com/dotnet/spark/pull/561))

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
            <td rowspan=4>v0.9.0</td>
            <td>DataFrame with Grouped Map UDF <a href="https://github.com/dotnet/spark/pull/277">(#277)</a></td>
        </tr>
        <tr>
            <td>DataFrame with Vector UDF <a href="https://github.com/dotnet/spark/pull/277">(#277)</a></td>
        </tr>
        <tr>
            <td>Support for Broadcast Variables <a href="https://github.com/dotnet/spark/pull/414">(#414)</a></td>
        </tr>
        <tr>
            <td>Support for TimestampType <a href="https://github.com/dotnet/spark/pull/428">(#428)</a></td>
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
            <td>v0.9.0</td>
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
            <td>microsoft-spark-2.3.x-0.12.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=6>microsoft-spark-2.4.x-0.12.0.jar</td>
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
            <td>2.4.2</td>
            <td><a href="https://github.com/dotnet/spark/issues/60">Not supported</a></td>
        </tr>
    </tbody>
</table>
