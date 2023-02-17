# .NET for Apache Spark Roadmap

The goal of the .NET for Apache Spark project is to provide an easy to use, .NET-friendly integration to the popular big data platform, Apache Spark. This document describes the tentative plan for the project in the short and long-term. 

.NET for Apache Spark is a community effort and we welcome community feedback on our plans. The best way to give feedback is to open an issue in this repo. We are also excited to receive contributions (check out the [contribution guide](docs/contributing.md)). It's always a good idea to open an issue for a discussion before embarking on a large code change to make sure there is not duplicated effort. Where we do know that efforts are already underway, we have used the (*) marker below.

## Short Term

### User Experience
* 1:1 API compatibility for Dataframes with Apache Spark 2.3.x, Apache Spark 2.4.x and Apache Spark 3.0.x (*)

### Performance Optimizations
* Improvements to C# Pickling Library
* Improvements to Arrow .NET Library
* Exploiting .NET Vectorization (*)
* Micro-benchmarking framework for Interop

### Benchmarks
* Benchmarking scripts for all languages that include generating the dataset and running queries against it (*)
* Published reproducible benchmarks against [TPC-H](http://www.tpc.org/tpch/) (industry-standard database benchmark) (*)

### Tooling Improvements
* VS Code support (*)
* Apache Jupyter integration with C# & F# Notebook Support (*)
* Improved user experience for .NET app submission to a remote Spark cluster

## Longer Term

### User Experience
* Idiomatic C# and F# APIs

### Performance Optimizations
* Contribute extensible interop layer to Apache Spark

### Benchmarks
* Published reproducible benchmarks against [TPC-DS](http://www.tpc.org/tpcds/default.asp) (industry-standard database benchmark)

### Tooling Improvements
* Visual Studio Extension for .NET app submission to a remote Spark cluster
* Visual Studio Extension for .NET app debugging
* Make it easy to copy/paste Scala examples into Visual Studio
