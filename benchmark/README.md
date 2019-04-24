Benchmarking
===

# Generate Data
> *TODO* instructions to be provided
# Instructions
## CSharp
1. Ensure that the Microsoft.Spark.Worker is properly [installed](https://github.com/dotnet/spark/tree/master/deployment#cloud-deployment) in your cluster.
2. Build `microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar` and the [csharp tpch benchmark](csharp/Tpch) application by following the [build instructions](../docs/building).
3. Upload [run_csharp_benchmark.sh](run_csharp_benchmark.sh), the Tpch benchmark application, and `microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar` to the cluster.
4. Run the benchmark by invoking `run_csharp_benchmark.sh`
    ```shell
    run_csharp_benchmark.sh \
    <num_executors> \
    <driver_memory> \
    <executor_memory> \
    <executor_cores> \
    </path/to/Tpch.dll> \
    </path/to/microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar> \
    </path/to/Tpch executable> \
    </path/to/dataset> \
    <number of iterations> \
    <true for sql tests, false for functional tests>
    ```

## Python
1. Upload [run_python_benchmark.sh](run_python_benchmark.sh) and all [python tpch benchmark](python/) files to the cluster.
2. Run the benchmark by invoking `run_python_benchmark.sh`
    ```shell
    run_python_benchmark.sh \
    <num_executors> \
    <driver_memory> \
    <executor_memory> \
    <executor_cores> \
    </path/to/tpch.py> \
    </path/to/dataset> \
    <number of iterations> \
    <true for sql tests, false for functional tests>
    ```

## Scala
1. `mvn package` to build the [scala tpch benchmark](scala/) application.
2. Upload [run_scala_benchmark.sh](run_scala_benchmark.sh) and the `microsoft-spark-benchmark-<version>.jar` to the cluster.
3. Run the benchmark by invoking `run_scala_benchmark.sh`
    ```shell
    run_scala_benchmark.sh \
    <num_executors> \
    <driver_memory> \
    <executor_memory> \
    <executor_cores> \
    </path/to/microsoft-spark-benchmark-<version>.jar> \
    </path/to/dataset> \
    <number of iterations> \
    <true for sql tests, false for functional tests>
    ```