# .NET for Apache Spark 0.4 Release Notes

### Release Notes

Below are some of the highlights from this release:
* Revamped loading assemblies used inside UDFs ([#180](https://github.com/dotnet/spark/pull/180))
* Support for Vector UDFs ([#127](https://github.com/dotnet/spark/pull/127))
* Support for Grouped Map UDFs([#143](https://github.com/dotnet/spark/pull/143))
* Ability to launch a debugger from the worker ([#150](https://github.com/dotnet/spark/pull/150))
* Compatibility check for Microsoft.Spark.dll in the worker ([#170](https://github.com/dotnet/spark/pull/170))
* Update Apache.Arrow to v0.14.1 ([#167](https://github.com/dotnet/spark/pull/167))
* Support for RuntimeConfig in SparkSession ([#184](https://github.com/dotnet/spark/pull/184))
* Resloving signer mismatch issue ([#186](https://github.com/dotnet/spark/pull/186))
* Support for `Trigger` in `DataStreamWriter` ([#153](https://github.com/dotnet/spark/pull/153))
* The ability to use `--archives` option to deploy the worker binaries and assemblies that UDFs depend on ([#187](https://github.com/dotnet/spark/pull/187))

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
            <td rowspan=3>microsoft-spark-2.4.x-0.4.0.jar</td>
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
