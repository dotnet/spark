using Microsoft.Spark.Extensions.Hadoop.FileSystem;
using Microsoft.Spark.Sql;
using System;

namespace SparkDotnet
{
    class Program
    {
        static void Main(string[] args)
        {
            SparkSession spark = SparkSession
                .Builder()
                .AppName("Test FileSystem")
                .GetOrCreate();

            using FileSystem fs = FileSystem.Get(spark.SparkContext);

            string path = $"/file-system-test/{Guid.NewGuid()}/";
            spark.Range(25).Write().Format("parquet").Save(path);

            Console.WriteLine(path);
            Console.WriteLine(fs.Delete(path, true));
            Console.WriteLine(fs.Delete(path, true));

            spark.Stop();
        }
    }
}
