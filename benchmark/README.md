Benchmarking
===

# Generate Data
1. [Download](http://www.tpc.org/tpc_documents_current_versions/current_specifications.asp) the TPC-H benchmark tool.
Follow the instructions for registration and download the tool to local disk with at least 300GB free space.

2. Build the dbgen tool.
    - Decompress the zip file, then navigate to `dbgen` folder.
    - For Linux, the TPC-H README contains instructions on how to build the tool.
    - For Windows, generate the dbgen.exe using Visual Studio:
        - (1). In the `dbgen` folder, you will see `tpch.sln`, open it using Visual Studio.
        - (2). Build `dbgen` project, no need to build `qgen`, it should generate `dbgen.exe` in the `Debug` folder.

3. Generate the data.
    - For Linux, the TPC-H README contains instructions on how to generate the database tables.
    - For Windows,
        - (1). Copy `dbgen.exe` to the `dbgen` folder
        - (2). The following will generate a 300GB TPC-H dataaset:
        ```shell
        cd /d \path\to\dbgen
        dbgen.exe -vf -s 300
        ```
        *Note*: Since there is no parallelization option for TPC-H dbgen, generating a 300GB dataset could take up to 40 hours to complete.

    - After database population generation is completed, there should be 8 tables (customer, lineitem, nation, orders, part, partsupp, region, supplier) created with the .tbl extension.

4. Convert TPC-H dataset to parquet format.
    - You can use a simple Spark [application](https://github.com/dotnet/spark/blob/master/benchmark/scala/src/main/scala/com/microsoft/tpch/ConvertTpchCsvToParquetApp.scala) to convert the TPC-H dataset to parquet format. You can run the following spark-submit command to submit the application, you can also adjust it according to format of [submitting application](https://spark.apache.org/docs/latest/submitting-applications.html).
```
        <spark-submit> --master local[*] --class com.microsoft.tpch.ConvertTpchCsvToParquetApp microsoft-spark-benchmark-<version>.jar <path-to-source-directory-with-TPCH-tables> <path-to-destination-directory-to-save-parquet-file>
```

# Cluster Run
TPCH timing results is written to stdout in the following form: `TPCH_Result,<language>,<test type>,<query number>,<iteration>,<total time taken for iteration in milliseconds>,<time taken to run query in milliseconds>`

- Cold Run
   - Each <query + iteration> uses a new spark-submit
- Warm Run
   - Each query uses a new spark-submit
   - Each iteration reuses the Spark Session after creating the Dataframe (therefore, skips the load phase that does file enumeration)

## CSharp
1. Ensure that the Microsoft.Spark.Worker is properly [installed](../deployment/README.md#cloud-deployment) in your cluster.
2. Build `microsoft-spark-<version>.jar` and the [CSharp Tpch benchmark](csharp/Tpch) application by following the [build instructions](../README.md#building-from-source).
3. Upload [run_csharp_benchmark.sh](run_csharp_benchmark.sh), the Tpch benchmark application, and `microsoft-spark-<version>.jar` to the cluster.
4. Run the benchmark by invoking:
    ```shell
    run_csharp_benchmark.sh \
    <number of cold iterations> \
    <num_executors> \
    <driver_memory> \
    <executor_memory> \
    <executor_cores> \
    </path/to/Tpch.dll> \
    </path/to/microsoft-spark-<version>.jar> \
    </path/to/Tpch executable> \
    </path/to/dataset> \
    <number of iterations> \
    <true for sql tests, false for functional tests>
    ```

    **Note**: Ensure that you build the worker and application with .NET 8 in order to run hardware acceleration queries.

## Python
1. Upload [run_python_benchmark.sh](run_python_benchmark.sh) and all [python tpch benchmark](python/) files to the cluster.
2. Install pyarrow and pandas on all nodes in the cluster. For example, if you are using Conda, you can use the following commands to install them.
    ```shell
    sudo /path/to/conda update --all
    sudo /path/to/conda install pandas
    sudo /path/to/conda install pyarrow
    ```
3. Run the benchmark by invoking:
    ```shell
    run_python_benchmark.sh \
    <number of cold iterations> \
    <num_executors> \
    <driver_memory> \
    <executor_memory> \
    <executor_cores> \
    </path/to/tpch.py> \
    </path/to/dataset> \
    <number of iterations> \
    <true for sql tests, false for functional tests>
    ```
In order to run with Python 3.x (the default is 2.7) you will need to do the following on the driver node.
1. Activate the Python 3.x environment, by changing the value of PYSPARK_PYTHON environment variable to point to the Python3 binary. This change can be made in the spark-env.sh conf file.
    ```shell
    export PYSPARK_PYTHON=${PYSPARK_PYTHON:-/path/to/python3}
    ```

## Scala
1. `mvn package` to build the [scala tpch benchmark](scala/) application.
2. Upload [run_scala_benchmark.sh](run_scala_benchmark.sh) and the `microsoft-spark-benchmark-<version>.jar` to the cluster.
3. Run the benchmark by invoking:
    ```shell
    run_scala_benchmark.sh \
    <number of cold iterations> \
    <num_executors> \
    <driver_memory> \
    <executor_memory> \
    <executor_cores> \
    </path/to/microsoft-spark-benchmark-<version>.jar> \
    </path/to/dataset> \
    <number of iterations> \
    <true for sql tests, false for functional tests>
    ```
