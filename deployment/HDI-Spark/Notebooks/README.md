# Deploy SparkDotnet REPL to HDI Spark Cluster with Notebook Experience

## Goal

This documentation will elaborate the steps on how to deploy SparkDotnet REPL to HDI Spark Cluster and submit jobs through Jupyter Notebook using SparkDotnet.

## Background

We are currently using [dotnet-try](https://github.com/dotnet/try) as our dotnet REPL. ``` dotnet-try ``` can be used as a jupyter kernel and we implement it through [sparkmagic](https://github.com/jupyter-incubator/sparkmagic) and livy. To enable Jupyter Notebook Experience using SparkDotnet, this will include simple manual steps through [Ambari](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-manage-ambari) and [script actions](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-customize-cluster-linux) in HDI Spark Cluster.

## Step

### 1. Go to Ambari, manually stop Livy Server on head node.

Details to be added.

### 2. Run Script Actions on head node and worker node.

Details to be added.

### 3. Go to Ambari, manually start Livy Server on head node.

Details to be added.

### 4. Ambari -> Spark2 -> CONFIGS, set up Custom spark2-defaults.

Details to be added.

## Submit Jobs through Jupyter Notebook

Examples

## Conclusion

Some conclusion here.

## What's Next

More things to consider in the long-term run...
