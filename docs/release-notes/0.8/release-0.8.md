# .NET for Apache Spark 0.8 Release Notes

### New Features and Improvements

* Expose `SparkContext.setLogLevel()` to change log level programmatically ([#360](https://github.com/dotnet/spark/pull/360))
* Expose `Schema()` for `DataFrameReader` and `DataStreamReader`([#248](https://github.com/dotnet/spark/pull/248))
* Expose `Column.Apply()` ([#323](https://github.com/dotnet/spark/pull/323))
* "Deprecated" annotation is added to the following functions:
  * `WindowSpec.RangeBetween(Column start, Column end)`
  * `Window.RangeBetween(Column start, Column end)`
  * `Functions.UnboundedPreceding()`
  * `Functions.UnboundedFollowing()`
  * `Functions.CurrentRow()`

### Breaking Changes
* The following APIs have been removed due to the thread-local variable dependency (see [#332](https://github.com/dotnet/spark/pull/332) and [#333](https://github.com/dotnet/spark/issues/333) for more detail):
  * `SparkSession.ClearActiveSession()`
  * `SparkSession.GetActiveSession()`
  * `SparkSession.SetActiveSession()`

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
            <td rowspan=4>microsoft-spark-2.4.x-0.8.0.jar</td>
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
