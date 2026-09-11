# Retired Spark 2.4 / HDInsight 4.0 notebook integration

The experimental Spark .NET notebook installer for Spark 2.4 and HDInsight 4.0 is retired with the removal of Spark 2.x support. The installer is no longer shipped in the current source tree.

The [historical instructions and installer](https://github.com/dotnet/spark/tree/30e0ef87f79ea2a260c48001a455979b1e14dacb/deployment/HDI-Spark/Notebooks) remain available for understanding existing deployments. They replace Livy and SparkMagic with old patched binaries and depend on Scala 2.11, Python 2.7, and an older .NET REPL. Do not apply those instructions to a Spark 3.x or 4.0 cluster or substitute a newer bridge JAR and assume compatibility.

No replacement HDInsight notebook deployment has been validated here. Migrating this integration requires compatible Livy, Scala, kernel, REPL, and Worker components plus deployment testing on the chosen HDInsight runtime. The [generic deployment guide](../../README.md) describes Worker installation and application submission, not a validated notebook replacement. See the [Spark 2.x migration guide](../../../docs/migration-guide.md#upgrading-after-spark-2x-support-removal) for application changes.
