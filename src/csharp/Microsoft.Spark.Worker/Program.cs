// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

// TODO: Move the following to the AssemblyInfo.cs.
[assembly: InternalsVisibleTo("Microsoft.Spark.Worker.UnitTest, PublicKey=002400000480000094000000060200000024000052534131000400000100010015c01ae1f50e8cc09ba9eac9147cf8fd9fce2cfe9f8dce4f7301c4132ca9fb50ce8cbf1df4dc18dd4d210e4345c744ecb3365ed327efdbc52603faa5e21daa11234c8c4a73e51f03bf192544581ebe107adee3a34928e39d04e524a9ce729d5090bfd7dad9d10c722c0def9ccc08ff0a03790e48bcd1f9b6c476063e1966a1c4")]

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
