# .NET for Apache Spark 0.2 Release Notes

### Release Notes

Below are some of the highlights from this release.

* Added XML documentation file to Nuget package and the comments on public APIs are now visible.
* Bug fixes:
    * `DataFrameWriter.Option()` APIs were not setting keys correctly. ([#59](https://github.com/dotnet/spark/pull/59))
    * `DataFrameWriter.PartitionBy()` is fixed ([#56](https://github.com/dotnet/spark/pull/56)) 
    
### Supported Spark Versions

The following table outlines the supported Spark versions along with the microsoft-spark JAR to use with:

<table>
    <thead>
        <tr>
            <th>Spark Version</th>
            <th>microsoft-spark JAR</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2.3.0</td>
            <td rowspan=4>microsoft-spark-2.3.x-0.2.0.jar</td>
        </tr>
        <tr>
            <td>2.3.1</td>
        </tr>
        <tr>
            <td>2.3.2</td>
        </tr>
        <tr>
            <td>2.3.3</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=4>microsoft-spark-2.4.x-0.2.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
        </tr>
        <tr>
            <td>2.4.1</td>
        </tr>
    </tbody>
</table>
