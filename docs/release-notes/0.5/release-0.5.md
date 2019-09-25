# .NET for Apache Spark 0.5 Release Notes

### New Features and Improvements

* Support for `DeltaTable` APIs in [Delta Lake](https://github.com/delta-io/delta) ([#236](https://github.com/dotnet/spark/pull/236))
* Support for Spark 2.3.4/2.4.4 ([#232](https://github.com/dotnet/spark/pull/232), [#240](https://github.com/dotnet/spark/pull/240))
* Support for UDF taking a `Row` object as input ([#214](https://github.com/dotnet/spark/pull/214))
* Exposing new APIs:
   * `SparkSession.Catalog` ([#231](https://github.com/dotnet/spark/pull/231))
   * `SparkFiles` ([#255](https://github.com/dotnet/spark/pull/255))
   * `SparkSession.Range` ([#225](https://github.com/dotnet/spark/pull/225))
   * `Column.IsIn` ([#171](https://github.com/dotnet/spark/pull/171))
* Ground work to support [dotnet-try](https://github.com/dotnet/try) as C# REPL ([#251](https://github.com/dotnet/spark/pull/251))
* Bug fix in generating Nuget package ([#234](https://github.com/dotnet/spark/pull/234))
* Support for `MapType` ([#235](https://github.com/dotnet/spark/pull/235))
* Examples of using .NET Core 3.0 hardware intrinsics ([#211](https://github.com/dotnet/spark/pull/211))

### Breaking Changes
* None, but the new `Microsoft.Spark.Worker` needs to be used to enable UDF taking `Row` object as input ([#214](https://github.com/dotnet/spark/pull/214)).

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
            <td>microsoft-spark-2.3.x-0.4.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=4>microsoft-spark-2.4.x-0.4.0.jar</td>
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
            <td>2.4.2</td>
            <td><a href="https://github.com/dotnet/spark/issues/60">Not supported</a></td>
        </tr>
    </tbody>
</table>
