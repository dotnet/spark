# .NET for Apache Spark 1.0.0 Release Notes

### New Features/Improvements

* DataFrame API Completeness for Spark 2.4.x ([#621](https://github.com/dotnet/spark/pull/621))([#598](https://github.com/dotnet/spark/pull/598))([#623](https://github.com/dotnet/spark/pull/623))([#635](https://github.com/dotnet/spark/pull/635))([#642](https://github.com/dotnet/spark/pull/642))([#665](https://github.com/dotnet/spark/pull/665))([#698](https://github.com/dotnet/spark/pull/698))
* DataFrame API Completeness for Spark 3.0.x ([#633](https://github.com/dotnet/spark/pull/633))([#647](https://github.com/dotnet/spark/pull/647))([#649](https://github.com/dotnet/spark/pull/649))([#655](https://github.com/dotnet/spark/pull/655))([#677](https://github.com/dotnet/spark/pull/677))([#688](https://github.com/dotnet/spark/pull/688))
* Expose more `Microsoft.Spark.ML.Feature` classes ([#574](https://github.com/dotnet/spark/pull/574))([#586](https://github.com/dotnet/spark/pull/586))([#608](https://github.com/dotnet/spark/pull/608))([#652](https://github.com/dotnet/spark/pull/652))([#703](https://github.com/dotnet/spark/pull/703))
* Override `Microsoft.Spark.Sql.Column.ToString()` to call Java's `toString()` ([#624](https://github.com/dotnet/spark/pull/624))
* Refactor `Microsoft.Spark.Sql.ArrowArrayHelpers` ([#636](https://github.com/dotnet/spark/pull/636))([#646](https://github.com/dotnet/spark/pull/646))
* Ensure all calls from a given thread in the CLR is always executed in the same thread in the JVM  ([#641](https://github.com/dotnet/spark/pull/641))
* Support `Microsoft.Spark.Sql.Types.ArrayType` and `Microsoft.Spark.Sql.Types.MapType` ([#670](https://github.com/dotnet/spark/pull/670))([#689](https://github.com/dotnet/spark/pull/689))
* Expose `Microsoft.Spark.Sql.Streaming.StreamingQueryManager` ([#670](https://github.com/dotnet/spark/pull/670))([#690](https://github.com/dotnet/spark/pull/690))
* Update to `Microsoft.Dotnet.Interactive` 1.0.0-beta.20480.3 in `Microsoft.Spark.Extensions.DotNet.Interactive` ([#694](https://github.com/dotnet/spark/pull/694))
* Broadcast encryption support ([#489](https://github.com/dotnet/spark/pull/489))

### Bug Fixes

* Fix concurrent reading of broadcast variable file during deserialization ([612](https://github.com/dotnet/spark/pull/612))
* Return UTF-8 encoded string from JVM => CLR ([661](https://github.com/dotnet/spark/pull/661))
* Add JVM CallbackClient connection back to connection pool ([681](https://github.com/dotnet/spark/pull/681))
* JvmBridge/Netty blocking connection deadlock mitigation ([714](https://github.com/dotnet/spark/pull/714))

### Infrastructure / Documentation / Etc.

* Update Delta Lake tests against Delta Lake 0.6.1 ([#588](https://github.com/dotnet/spark/pull/588))
* Add more `DataFrame` examples ([#599](https://github.com/dotnet/spark/pull/599))
* Update ubuntu instructions by prepending current directory in sample command ([#603](https://github.com/dotnet/spark/pull/603))
* [Delta Lake](https://github.com/delta-io/delta) version annotations to `Microsoft.Spark.Extensions.Delta` ([#632](https://github.com/dotnet/spark/pull/632))
* [Hyperspace](https://github.com/microsoft/hyperspace) version annotations to `Microsoft.Spark.Extensions.Hyperspace` ([#696](https://github.com/dotnet/spark/pull/696))
* Refactor unit tests ([#638](https://github.com/dotnet/spark/pull/638))
* Add `SimpleWorkerTests` ([#644](https://github.com/dotnet/spark/pull/644))
* Update to `Arrow` 0.15.1 ([#653](https://github.com/dotnet/spark/pull/653))
* Remove `Microsoft.Spark.Extensions.Azure.Synapse.Analytics` from repo ([#687](https://github.com/dotnet/spark/pull/687))
* Move `Microsoft.Spark.Experimental` to `Microsoft.Spark` ([#691](https://github.com/dotnet/spark/pull/691))
* Fix `DaemonWorkerTests.TestsDaemonWorkerTaskRunners` and `CallbackTests.TestCallbackServer` tests ([#699](https://github.com/dotnet/spark/pull/699))
* Improve build pipeline ([#667](https://github.com/dotnet/spark/pull/667))([#692](https://github.com/dotnet/spark/pull/692))([#697](https://github.com/dotnet/spark/pull/697))([#705](https://github.com/dotnet/spark/pull/705))([#717](https://github.com/dotnet/spark/pull/717))([#719](https://github.com/dotnet/spark/pull/719))([#720](https://github.com/dotnet/spark/pull/720))


### Breaking Changes

* No backward/forward compatibility between `Microsoft.Spark` and `Microsoft.Spark.Worker` if `version` < 1.0.0

### Known Issues

* Broadcast variables do not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#561](https://github.com/dotnet/spark/pull/561))
* GroupedMap `not` supported on Spark 3.0.0 ([#654](https://github.com/dotnet/spark/issues/654))

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
            <td>microsoft-spark-2.3.x-1.0.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=7>microsoft-spark-2.4.x-1.0.0.jar</td>
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
            <td>2.4.7</td>
        </tr>
        <tr>
            <td>2.4.2</td>
            <td><a href="https://github.com/dotnet/spark/issues/60">Not supported</a></td>
        </tr>
        <tr>
            <td>3.0.0</td>
            <td rowspan=2>microsoft-spark-3.0.x-1.0.0.jar</td>
        </tr>
        <tr>
            <td>3.0.1</td>
        </tr>
    </tbody>
</table>
