# GitHub Project Analysis with Batch Data

In this sample, you'll see how to use [.NET for Apache Spark](https://dotnet.microsoft.com/apps/data/spark) 
to analyze a file containing information about a set of GitHub projects. 

## Problem

Our goal here is to analyze information about GitHub projects. We start with some data prep (removing null data and unnecessary columns) 
and then perform further analysis about the number of times different projects have been forked and how recently different projects
have been updated.

This sample is an example of **batch processing** since we're analyzing data that has already been stored and is not actively growing 
or changing.

## Dataset

The data used in this example was generated from [GHTorrent](http://ghtorrent.org/), which monitors all public GitHub events (such as info about projects, commits, and watchers), stores the events and their structure in databases, and then releases data collected over different time periods as downloadable archives. 

The dataset used when creating this sample was [downloaded from the GHTorrent archives](http://ghtorrent.org/downloads.html). Specifically, the **projects.csv** file was extracted from one of the latest MySQL dumps. For analysis that only takes a few seconds in demos, projects.csv was shortened and thus the dataset is called **[projects_smaller.csv](../projects_smaller.csv)** throughout this sample.

The GHTorrent dataset is distributed under a dual licensing scheme ([Creative Commons +](https://wiki.creativecommons.org/wiki/CCPlus)). For non-commercial uses (including, but not limited to, educational, research or personal uses), the dataset is distributed under the [CC-BY-SA license](https://creativecommons.org/licenses/by-sa/4.0/).

## Solution

Let's explore how we can use Spark to tackle this problem.

### 1. Create a Spark Session

In any Spark application, we need to establish a new SparkSession, which is the entry point to programming Spark with the Dataset and 
DataFrame API.

```CSharp
SparkSession spark = SparkSession
                .Builder()
                .AppName("GitHub and Spark Batch")
                .GetOrCreate();
```

By calling on the *spark* object created above, we can access Spark and DataFrame functionality throughout our program.

### 2. Read Input File into a DataFrame

Now that we have an entry point to the Spark API, let's read in our GitHub projects file. We'll store it in a `DataFrame`, which is a distributed collection of data organized into named columns.

```CSharp
DataFrame df = spark.Read().Text("Path to input data set");
```

### 3. Data Prep

We can use `DataFrame.Na()` to drop rows with NA (null) values, and the `DataFrameNaFunctions.Drop()` method to remove certain columns from our data. 
This helps prevent errors if we try to analyze null data or columns that do not matter for our final analysis.

### 4. Further Analysis

Spark SQL allows us to make SQL calls on our data. It is common to combine UDFs and Spark SQL so that we can apply a UDF to all 
rows of our DataFrame.

We can specifically call `spark.Sql` to mimic standard SQL calls seen in other types of apps, and we can also call methods like 
`GroupBy` and `Agg` to specifically combine, filter, and perform calculations on our data.

### 5. Running Your App

Checkout the directions for building and running this app on [Windows](../../../../../docs/building/windows-instructions.md) or [Ubuntu](../../../../../docs/building/ubuntu-instructions.md).

#### Windows Example:

```powershell
spark-submit --class org.apache.spark.deploy.dotnet.DotnetRunner --master local /path/to/microsoft-spark-<version>.jar Microsoft.Spark.CSharp.Examples.exe Sql.Batch.GitHubProjects /path/to/projects_smaller.csv
```

> **Note:** Be sure to update the above command with the actual paths to your Microsoft Spark jar file and **projects_smaller.csv**.

## Next Steps

View the [full coding example](../GitHubProjects.cs) to see an example of prepping and analyzing GitHub data.

You can also view a live video explanation of this app and batch processing overall in the [.NET for Apache Spark 101 video series](https://www.youtube.com/watch?v=i_NvL8p_KZg&list=PLdo4fOcmZ0oXklB5hhg1G1ZwOJTEjcQ5z&index=4&t=3s).
