# .NET for Apache Spark 0.2 Release Notes

Below are some of the highlights from this release.

* Added XML documentation file to Nuget package and the comments on public APIs are now visible.
* Bug fixes:
    * `DataFrameWriter.Option()` APIs were not setting keys correctly. ([#59](https://github.com/dotnet/spark/pull/59))
    * `DataFrameWriter.PartitionBy()` is fixed ([#56](https://github.com/dotnet/spark/pull/56)) 
