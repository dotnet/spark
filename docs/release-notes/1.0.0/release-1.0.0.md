# .NET for Apache Spark 1.0.0 Release Notes

### New Features/Improvements

* DataFrame API Completeness for Spark 2.4.x ([#621](https://github.com/dotnet/spark/pull/621))([#598](https://github.com/dotnet/spark/pull/598))([#623](https://github.com/dotnet/spark/pull/623))([#635](https://github.com/dotnet/spark/pull/635))([#642](https://github.com/dotnet/spark/pull/642))([#665](https://github.com/dotnet/spark/pull/665))([#698](https://github.com/dotnet/spark/pull/698))
* DataFrame API Completeness for Spark 3.0.x ([#633](https://github.com/dotnet/spark/pull/633))([#647](https://github.com/dotnet/spark/pull/647))([#649](https://github.com/dotnet/spark/pull/649))([#655](https://github.com/dotnet/spark/pull/655))([#677](https://github.com/dotnet/spark/pull/677))([#688](https://github.com/dotnet/spark/pull/688))
* Expose more `Microsoft.Spark.ML.Feature` classes ([#574](https://github.com/dotnet/spark/pull/574))([#586](https://github.com/dotnet/spark/pull/586))([#608](https://github.com/dotnet/spark/pull/608))([#652](https://github.com/dotnet/spark/pull/652))([#703](https://github.com/dotnet/spark/pull/703))
* Support `Microsoft.Spark.Sql.Types.ArrayType` and `Microsoft.Spark.Sql.Types.MapType` ([#670](https://github.com/dotnet/spark/pull/670))([#689](https://github.com/dotnet/spark/pull/689))
* Ensure all calls from a given thread in the CLR is always executed in the same thread in the JVM  ([#641](https://github.com/dotnet/spark/pull/641))
* Expose `Microsoft.Spark.Sql.Streaming.StreamingQueryManager` ([#670](https://github.com/dotnet/spark/pull/670))([#690](https://github.com/dotnet/spark/pull/690))
* Broadcast encryption support ([#489](https://github.com/dotnet/spark/pull/489))
* Support for Delta Lake 0.7.0 ([#692](https://github.com/dotnet/spark/pull/692))([#727](https://github.com/dotnet/spark/pull/727))
* Helper method that returns a `DataFrame` containing the `Microsoft.Spark` and `Microsoft.Spark.Worker` assembly version info. ([#715](https://github.com/dotnet/spark/pull/715))
* Update version check logic on the `Microsoft.Spark.Worker`. ([#718](https://github.com/dotnet/spark/pull/718))
* Update to `Microsoft.Dotnet.Interactive` 1.0.0-beta.20480.3 in `Microsoft.Spark.Extensions.DotNet.Interactive` ([#694](https://github.com/dotnet/spark/pull/694))
* Override `Microsoft.Spark.Sql.Column.ToString()` to call Java's `toString()` ([#624](https://github.com/dotnet/spark/pull/624))
* Refactor `Microsoft.Spark.Sql.ArrowArrayHelpers` ([#636](https://github.com/dotnet/spark/pull/636))([#646](https://github.com/dotnet/spark/pull/646))

### Bug Fixes

* JvmBridge/Netty blocking connection deadlock mitigation ([#714](https://github.com/dotnet/spark/pull/714), [#735](https://github.com/dotnet/spark/pull/735))
* Fix concurrent reading of broadcast variable file during deserialization ([#612](https://github.com/dotnet/spark/pull/612))
* Return UTF-8 encoded string from JVM => CLR ([#661](https://github.com/dotnet/spark/pull/661))
* Add JVM CallbackClient connection back to connection pool ([#681](https://github.com/dotnet/spark/pull/681))

### Infrastructure / Documentation / Etc.

* Update Delta Lake tests against Delta Lake 0.6.1 and Delta Lake 0.7.0/Spark 3.0 ([#588](https://github.com/dotnet/spark/pull/588))([#692](https://github.com/dotnet/spark/pull/692))
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
* Improve build pipeline ([#348](https://github.com/dotnet/spark/pull/348))([#667](https://github.com/dotnet/spark/pull/667))([#692](https://github.com/dotnet/spark/pull/692))([#697](https://github.com/dotnet/spark/pull/697))([#705](https://github.com/dotnet/spark/pull/705))([#717](https://github.com/dotnet/spark/pull/717))([#719](https://github.com/dotnet/spark/pull/719))([#720](https://github.com/dotnet/spark/pull/720))([#724](https://github.com/dotnet/spark/pull/724))
* `microsoft-spark` JAR renamed. ([#293](https://github.com/dotnet/spark/pull/293))([#728](https://github.com/dotnet/spark/pull/728))([#729](https://github.com/dotnet/spark/pull/729))


### Breaking Changes

* Prior versions (`version` < `1.0`) of `Microsoft.Spark` and `Microsoft.Spark.Worker` are no longer compatible with `1.0`. If you are planning to upgrade to `1.0`, please check the [migration guide](../../migration-guide.md#upgrading-from-microsoftspark-0x-to-10).
* `microsoft-spark` JAR name has changed. Please check the [migration guide](../../migration-guide.md#upgrading-from-microsoftspark-0x-to-10).

### Known Issues

* Broadcast variables do not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#561](https://github.com/dotnet/spark/pull/561))
* UDFs defined using class objects with closures does not work with [dotnet-interactive](https://github.com/dotnet/interactive) ([#619](https://github.com/dotnet/spark/pull/619))
* In [dotnet-interactive](https://github.com/dotnet/interactive) blocking Spark methods that require external threads to unblock them does not work. ie `StreamingQuery.AwaitTermination` requires `StreamingQuery.Stop` to unblock ([#736](https://github.com/dotnet/spark/pull/736))
* `GroupedMap` does not work on Spark 3.0.0 ([#654](https://github.com/dotnet/spark/issues/654))

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
            <td>microsoft-spark-2-3_2.11-1.0.0.jar</td>
        </tr>
        <tr>
            <td>2.4.0</td>
            <td rowspan=7>microsoft-spark-2-4_2.11-1.0.0.jar</td>
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
            <td rowspan=2>microsoft-spark-3-0_2.12-1.0.0.jar</td>
        </tr>
        <tr>
            <td>3.0.1</td>
        </tr>
    </tbody>
</table>

### Supported Delta Versions

The following table outlines the supported Delta versions along with the Microsoft.Spark.Extensions version to use with:

<table>
    <thead>
        <tr>
            <th>Delta Version</th>
            <th>Microsoft.Spark.Extensions.Delta</th>
        </tr>
    </thead>
    <tbody align="center">
        <tr>
            <td>0.1.0</td>
            <td rowspan=8>1.0.0</td>
        </tr>
        <tr>
            <td>0.2.0</td>
        </tr>
        <tr>
            <td>0.3.0</td>
        </tr>
        <tr>
            <td>0.4.0</td>
        </tr>
        <tr>
            <td>0.5.0</td>
        </tr>
        <tr>
            <td>0.6.0</td>
        </tr>
        <tr>
            <td>0.6.1</td>
        </tr>
        <tr>
            <td>0.7.0</td>
        </tr>
    </tbody>
</table>

### Supported Hyperspace Versions

The following table outlines the supported Hyperspace versions along with the Microsoft.Spark.Extensions version to use with:

<table>
    <thead>
        <tr>
            <th>Hyperspace Version</th>
            <th>Microsoft.Spark.Extensions.Hyperspace</th>
        </tr>
    </thead>
    <tbody align="center">
        <tr>
            <td>0.1.0</td>
            <td rowspan=2>1.0.0</td>
        </tr>
        <tr>
            <td>0.2.0</td>
        </tr>
    </tbody>
</table>
