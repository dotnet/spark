# .NET for Apache Spark 0.3 Release Notes

### Release Notes

Below are some of the highlights from this release.

* [Apache Spark 2.4.3](https://spark.apache.org/news/spark-2-4-3-released.html) support ([#118](https://github.com/dotnet/spark/pull/108))
* dotnet/spark is now using [dotnet/arcade](https://github.com/dotnet/arcade) as the build infrastructure ([#113](https://github.com/dotnet/spark/pull/113))
    * [Source Link](https://github.com/dotnet/sourcelink) is now supported for the Nuget package ([#40](https://github.com/dotnet/spark/issues/40)).
    * Fixed the issue where Microsoft.Spark.dll is not signed ([#119](https://github.com/dotnet/spark/issues/119)).
* Pickling performance is improved ([#111](https://github.com/dotnet/spark/pull/111)).
    * Performance improvment PRs in the Pickling Library: [irmen/Pyrolite#64](https://github.com/irmen/Pyrolite/pull/64), [irmen/Pyrolite#67](https://github.com/irmen/Pyrolite/pull/67)
* ArrayType and MapType are supported as UDF return types ([#112](https://github.com/dotnet/spark/issues/112#issuecomment-493297068), [#114](https://github.com/dotnet/spark/pull/114))

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
            <td>microsoft-spark-2.3.x-0.2.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=3>microsoft-spark-2.4.x-0.2.0.jar</td>
        </tr>
        <tr>
            <td>2.4.1</td>
        </tr>
        <tr>
            <td>2.4.3</td>
        </tr>
        <tr>
            <td>2.4.2</td>
            <td><a href="https://github.com/dotnet/spark/issues/60">Not supported</a></td>
        </tr>
    </tbody>
</table>
