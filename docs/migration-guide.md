# Migration Guide
- [Upgrading from Microsoft.Spark 0.x to 1.0](#upgrading-from-microsoftspark-0x-to-10)

## Upgrading from Microsoft.Spark 0.x to 1.0
- Limited support for [.NET Framework](https://dotnet.microsoft.com/learn/dotnet/what-is-dotnet-framework). Please migrate to [.NET Core >= 3.1](https://dotnet.microsoft.com/download/dotnet-core) instead.
- `Microsoft.Spark.Worker` should be upgraded to 1.0 as `Microsoft.Spark.Worker` 0.x is not forward-compatible with `Microsoft.Spark` 1.0.
- `Microsoft.Spark` should be upgraded to 1.0 as `Microsoft.Spark.Worker` 1.0 is not backward-compatible with `Microsoft.Spark` 0.x.
- `Microsoft.Spark.Experimental` project has been merged into `Microsoft.Spark`
  - `VectorUdf` from `Microsoft.Spark.Sql.ExperimentalFunctions` is now part of `Microsoft.Spark.Sql.ArrowFunctions`.
  - `VectorUdf` from `Microsoft.Spark.Sql.ExperimentalDataFrameFunctions` is now part of `Microsoft.Spark.Sql.DataFrameFunctions`.
  - Extension methods have been moved from `Microsoft.Spark.Sql.RelationalGroupedDatasetExtensions` into the main `Microsoft.Spark.Sql.RelationalGroupedDataset` class.
