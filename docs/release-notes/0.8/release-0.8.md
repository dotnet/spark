# .NET for Apache Spark 0.8 Release Notes

### New Features and Improvements

* Support for Row type Ser/De and exposing the CreateDataFrame API ([#338](https://github.com/dotnet/spark/pull/338))
* Support .NET Core 3.1 ([#291](https://github.com/dotnet/spark/pull/291) and [#386](https://github.com/dotnet/spark/pull/386))
* Support for new Delta v0.5.0 APIs ([#374](https://github.com/dotnet/spark/pull/374))
* Precompute the normalized type names of DataTypes and use string.Create on NS2.1 for faster normalization ([#364](https://github.com/dotnet/spark/pull/364))
* Support for Spark 2.4.5 ([#392](https://github.com/dotnet/spark/pull/392))

### Breaking Changes
* None

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
            <td>microsoft-spark-2.3.x-0.8.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=5>microsoft-spark-2.4.x-0.8.0.jar</td>
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
            <td>2.4.2</td>
            <td><a href="https://github.com/dotnet/spark/issues/60">Not supported</a></td>
        </tr>
    </tbody>
</table>
