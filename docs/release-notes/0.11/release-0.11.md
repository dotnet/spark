# .NET for Apache Spark 0.11 Release Notes

### New Features and Improvements

* Ability to pass and return corefxlab DataFrames to UDF APIs ([#277](https://github.com/dotnet/spark/pull/277))
* Supporting ML TF-IDF (Term frequency-inverse document frequency) feature vectorization method ([#394](https://github.com/dotnet/spark/pull/394))
* Support for TimestampType in `DataFrame.Collect()`, `CreateDataFrame` and UDFs ([#428](https://github.com/dotnet/spark/pull/428))
* Support for Broadcast Variables ([#414](https://github.com/dotnet/spark/pull/414))
* Implement ML feature Word2Vec ([#491](https://github.com/dotnet/spark/pull/491))
* Streamline logging when there is a failure ([#439](https://github.com/dotnet/spark/pull/439))


### Breaking Changes

* SparkSession.Catalog call changed from a method to a property ([#508](https://github.com/dotnet/spark/pull/508))

### Compatibility

The following table describes the oldest version of the worker that this current version is compatible with, excluding some incompatible features as shown below.

<table>
    <thead>
        <tr>
            <th>Oldest compatible .NET for Apache Spark worker</th>
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
            <td>microsoft-spark-2.3.x-0.11.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=5>microsoft-spark-2.4.x-0.11.0.jar</td>
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
