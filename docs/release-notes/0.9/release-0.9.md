# .NET for Apache Spark 0.9 Release Notes

### New Features and Improvements

* Expose `DataStreamWriter.Foreach` API ([#387](https://github.com/dotnet/spark/pull/387))
* Support UDF that returns `Row` object ([#376](https://github.com/dotnet/spark/pull/376), [#406](https://github.com/dotnet/spark/pull/406), [#411](https://github.com/dotnet/spark/pull/411))
* Support for Bucketizer ([#378](https://github.com/dotnet/spark/pull/378))

### Breaking Changes
* The new `Microsoft.Spark.Worker` needs to be used to enable UDF that returns `Row` object ([#376](https://github.com/dotnet/spark/pull/376)) and `DataStreamWriter.Foreach` ([#387](https://github.com/dotnet/spark/pull/387)).

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
            <td>microsoft-spark-2.3.x-0.9.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=4>microsoft-spark-2.4.x-0.9.0.jar</td>
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
