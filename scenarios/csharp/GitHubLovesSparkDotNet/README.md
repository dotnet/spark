In the **Github 💖 .NET for Apache Spark** app, we dive into the world of
analyzing GitHub meta-data using .NET for Apache Spark. While the analysis
is simple, it conveys a key point of how easy it is to get started with
analyzing large datasets using .NET.

# Table of Contents

- [Introduction](#introduction)
- [Technology Stacks](#technology-stacks)
- [Pre-requisites](#pre-requisites)
- [Running the App](#running-the-app)
- [Discussion](#discussion)
- [Insights](#insights)
- [Venues](#venues)
- [Acknowledgements](#acknowledgements)

# Introduction

[GHTorrent](http://ghtorrent.org/) monitors the [Github public event time line](https://api.github.com/events). 
For each event, it retrieves its contents and their dependencies, exhaustively. It 
then stores the data in a MySQL database. 

> **Disclaimer**: This app is purely for demonstration purposes. And more 
importantly, this was not written by a seasoned Data Scientist so please 
avoid deriving *any* conclusions from the results :) Of course, please feel 
free to open PRs to improve and make this app more interesting.

# Technology Stacks

This app utilizes and consequently familiarizes you with the following technology:

  - [Azure Databricks](https://azure.microsoft.com/en-us/services/databricks/)
  - [Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-us/services/storage/data-lake-storage/)
  - [Databricks CLI](https://docs.databricks.com/user-guide/dev-tools/databricks-cli.html)
  - [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/)
  - [Visual Studio Community Edition](https://visualstudio.microsoft.com/vs/community/)

# Pre-requisites

## Accounts & Cluster Creation

Make sure you have [created an Azure Databricks cluster and attached an Azure Data Lake Gen2 filesystem](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-use-databricks-spark) before you proceed.

## Dataset Preparation

  1. Download the latest MySQL CSV dataset from [GHTorrent Downloads page](http://ghtorrent.org/downloads.html)
  2. On your machine, untar (use [7-zip for Windows](https://www.7-zip.org/)) the dataset
     ```
     cd <path-to-mysql-csv-dump>
     tar xvzf mysql-2019-05-01.tar.gz
     ```
  3. Use [Azure Storage Explorer](https://azure.microsoft.com/en-us/features/storage-explorer/) to upload the dataset into your storage account

# Running the App

  1. Change the constants in `Program.cs` to reflect your account details
  2. Follow the [publish & deploy instructions for Azure Databricks](https://github.com/dotnet/spark/tree/master/deployment#databricks)

# Discussion

Work-in-progress

# Venues

This app was presented at Microsoft //Build 2019:

  1. By Scott Hanselman & Scott Hunter in the [BRK3015 - .NET Platform Overview and Roadmap](https://mybuild.techcommunity.microsoft.com/sessions/77031?source=sessions)
  2. By Rahul Potharaju in the [BRK3055 - Building data pipelines for Modern Data Warehouse with Spark and .NET in Azure](https://mybuild.techcommunity.microsoft.com/sessions/76996?source=sessions)

# Acknowledgements:

This app would not have been possible without work from the following:

  - [The GHTorrent project](http://ghtorrent.org)
  - [Azure Databricks](https://azure.microsoft.com/en-us/services/databricks/) 
  - [Azure Data Lake Storage Gen2 (ABFS)](https://azure.microsoft.com/en-us/services/storage/data-lake-storage/) 
  - Awesome people who helped put this together: Terry Kim, Steve Suh, Ankit Asthana, Michael Rys, Scott Hunter, Scott Hanselman
