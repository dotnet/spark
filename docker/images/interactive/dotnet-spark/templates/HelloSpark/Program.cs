using Microsoft.Spark.Sql;

namespace HelloSpark
{
    class Program
    {
        static void Main(string[] args)
        {
            var spark = SparkSession.Builder().GetOrCreate();
            var df = spark.Read().Json("people.json");
            df.Show();
        }
    }
}
