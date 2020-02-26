# .NET for Apache Spark 0.10 Release Notes

### New Features and Improvements

* Expose internal API to get Spark version on the driver side ([#429](https://github.com/dotnet/spark/pull/429))
* Add `SPARK_VERSION_OVERRIDE` environment variable ([#432](https://github.com/dotnet/spark/pull/432))

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
            <td>microsoft-spark-2.3.x-0.10.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=5>microsoft-spark-2.4.x-0.10.0.jar</td>
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
