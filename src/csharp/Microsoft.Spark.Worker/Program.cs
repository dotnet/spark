// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;

namespace Microsoft.Spark.Worker
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            var sparkVersion = new Version(
                Environment.GetEnvironmentVariable("DOTNET_WORKER_SPARK_VERSION"));

            // Note that for the daemon server, standard output is used to communicate
            // port number. Thus, use error output until the communication is complete.
            Console.Error.WriteLine(
                $"DotnetWorker PID:[{Process.GetCurrentProcess().Id}] " +
                $"Args:[{string.Join(" ", args)}] " +
                $"SparkVersion:[{sparkVersion}]");

#if NET8_0_OR_GREATER
            if (sparkVersion.Major == 4 && sparkVersion.Minor == 0 &&
                args.Length >= 2 && args[0] == "-m")
            {
                if (args[1] == "pyspark.daemon" &&
                    (args.Length == 2 || (args.Length == 3 && args[2] == "pyspark.worker")))
                {
                    new Spark40DaemonWorker().Run();
                    return;
                }

                if (args.Length == 2 && args[1] == Spark40DaemonWorker.ChildModule)
                {
                    Spark40DaemonWorker.WatchParent();
                    new SimpleWorker(sparkVersion).Run();
                    return;
                }
            }
#endif

            if (args.Length != 2)
            {
                Console.Error.WriteLine($"Invalid number of args: {args.Length}");
                Environment.Exit(-1);
            }

            if ((args[0] == "-m") && (args[1] == "pyspark.worker"))
            {
                new SimpleWorker(sparkVersion).Run();
            }
            else if ((args[0] == "-m") && args[1] == ("pyspark.daemon"))
            {
                new DaemonWorker(sparkVersion).Run();
            }
            else
            {
                Console.Error.WriteLine("Unknown options received.");
                Environment.Exit(-1);
            }
        }
    }
}
