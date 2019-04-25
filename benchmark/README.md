Benchmarking
===

# Generate Data
1. [Download](http://www.tpc.org/tpc_documents_current_versions/current_specifications.asp) the TPC-H benchmark tool.
Follow the registration instructions and download to local disk with at least 300GB free.
Follow the instructions for registration and download the tool and copy to local disk with free space larger than 300GB.

2. Build the dbgen tool.
- Decompress the zip file, then navigate to “dbgen” folder.
- The TPC-H README contains instructions on how to build the tool in Linux.
- For Windows users, you can generate the dbgen.exe using Visual Studio: 
   1. In the `dbgen` folder, you will see `tpch.sln`, open it using Visual Studio (VS2015 or VS2017 should work).
(b)	Click on tab “Build”, then “Build Solution”. 
(c)	Once the build is successful, it should generate "dbgen.exe" and "qgen.exe " in the “Debug” folder.

3. Generate the data.
- The TPC-H README contains instructions on how to generate a database population in Linux OS.
For Windows users, you can:
(a)	Copy “dbgen.exe” from “Debug” folder to the “dbgen” folder. 
(b)	Open cmd terminal, navigate to “dbgen” folder, and type command: dbgen -vf -s 300, which is generating 300GB TPCH dataset. It could take up to 40 hours to complete. Note that you can use command: dbgen -h, to check all the options. 
After database population generation is completed, you will see 8 tables with .tbl format:
    customer.tbl
    lineitem.tbl
    nation.tbl
    orders.tbl
    part.tbl
    partsupp.tbl
    region.tbl
    supplier.tbl

4. Convert to parquet format using a simple Spark application code. 
You can copy paste the application code below in your IDE, and run the job to convert the TPCH dataset to parquet format. 
> *TODO*: provide the link for the code. 

# Cluster Run
TPCH timing results is written to stdout in the following form: `TPCH_Result,<language>,<test type>,<query number>,<iteration>,<total time taken for iteration in milliseconds>,<time taken to run query in milliseconds>`

- Cold Run
   - Each <query + iteration> uses a new spark-submit
- Warm Run
   - Each query uses a new spark-submit
   - Each iteration reuses the Spark Session after creating the Dataframe (therefore, skips the load phase that does file enumeration)

## CSharp
1. Ensure that the Microsoft.Spark.Worker is properly [installed](../deployment/README.md#cloud-deployment) in your cluster.
2. Build `microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar` and the [CSharp Tpch benchmark](csharp/Tpch) application by following the [build instructions](../README.md#building-from-source).
3. Upload [run_csharp_benchmark.sh](run_csharp_benchmark.sh), the Tpch benchmark application, and `microsoft-spark-<spark_majorversion.spark_minorversion.x>-<spark_dotnet_version>.jar` to the cluster.
4. Run the benchmark by invoking:
    ```shell
    run_csharp_benchmark.sh \
    <number of cold iterations> \
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
2. Run the benchmark by invoking:
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
