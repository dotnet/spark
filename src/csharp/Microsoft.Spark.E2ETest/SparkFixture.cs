// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Runtime.InteropServices;
using Microsoft.Spark.Sql;
using Xunit;

namespace Microsoft.Spark.E2ETest
{
    /// <summary>
    /// SparkFixture acts as a global fixture to start Spark application in a debug
    /// mode through the spark-submit. It also provides a default SparkSession
    /// object that any tests can use.
    /// </summary>
    public class SparkFixture : IDisposable
    {
        private Process _process = new Process();

        internal SparkSession Spark { get; }

        public SparkFixture()
        {
            string workerDirEnvVarName = Services.ConfigurationService.WorkerDirEnvVarName;
#if NET461
            // Set the path for the worker executable to the location of the current
            // assembly since xunit framework copies the Microsoft.Spark.dll to an
            // isolated location for testing; the default mechanism of getting the directory
            // of the worker executable is the location of the Microsoft.Spark.dll.
            Environment.SetEnvironmentVariable(
                workerDirEnvVarName,
                AppDomain.CurrentDomain.BaseDirectory);
#elif NETCOREAPP2_1
            // For .NET Core, the user must have published the worker as a standalone
            // executable and set DotnetWorkerPath to the published directory.
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(workerDirEnvVarName)))
            {
                throw new Exception(
                    $"Environment variable '{workerDirEnvVarName}' must be set for .NET Core.");
            }
#else
            // Compile-time error for not supported frameworks.
            throw new NotSupportedException("Not supported frameworks.");
#endif
            BuildSparkCmd(out var filename, out var args);

            // Configure the process using the StartInfo properties.
            _process.StartInfo.FileName = filename;
            _process.StartInfo.Arguments = args;
            // UseShellExecute defaults to true in .NET Framework,
            // but defaults to false in .NET Core. To support both, set it
            // to false which is required for stream redirection.
            _process.StartInfo.UseShellExecute = false;
            _process.StartInfo.RedirectStandardInput = true;
            _process.StartInfo.RedirectStandardOutput = true;
            _process.StartInfo.RedirectStandardError = true;

            bool isSparkReady = false;
            _process.OutputDataReceived += (sender, arguments) =>
            {
                // Scala-side driver for .NET emits the following message after it is
                // launched and ready to accept connections.
                if (!isSparkReady &&
                    arguments.Data.Contains("Backend running debug mode"))
                {
                    isSparkReady = true;
                }
            };

            _process.Start();
            _process.BeginOutputReadLine();

            bool processExited = false;
            while (!isSparkReady && !processExited)
            {
                processExited = _process.WaitForExit(500);
            }

            if (processExited)
            {
                _process.Dispose();

                // The process should not have been exited.
                throw new Exception(
                    $"Process exited prematurely with '{filename} {args}'.");
            }

            Spark = SparkSession
                .Builder()
                .AppName("Microsoft.Spark.E2ETest")
                .GetOrCreate();
        }

        public void Dispose()
        {
            Spark.Dispose();

            // CSparkRunner will exit upon receiving newline from
            // the standard input stream.
            _process.StandardInput.WriteLine("done");
            _process.StandardInput.Flush();
            _process.WaitForExit();
        }

        private void BuildSparkCmd(out string filename, out string args)
        {
            string sparkHome = SparkSettings.SparkHome;

            // Build the executable name.
            char sep = Path.DirectorySeparatorChar;
            filename = $"{sparkHome}{sep}bin{sep}spark-submit";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                filename += ".cmd";
            }

            if (!File.Exists(filename))
            {
                throw new FileNotFoundException($"{filename} does not exist.");
            }

            // Build the arguments for the spark-submit.
            string classArg = "--class org.apache.spark.deploy.DotnetRunner";
            string curDir = AppDomain.CurrentDomain.BaseDirectory;
            string jarPrefix = GetJarPrefix(sparkHome);
            string scalaDir = $"{curDir}{sep}..{sep}..{sep}..{sep}..{sep}..{sep}src{sep}scala";
            string jarDir = $"{scalaDir}{sep}{jarPrefix}{sep}target";
            string assemblyVersion = Assembly.GetExecutingAssembly().GetName().Version.ToString(3);
            string jar = $"{jarDir}{sep}{jarPrefix}-{assemblyVersion}.jar";

            if (!File.Exists(jar))
            {
                throw new FileNotFoundException($"{jar} does not exist.");
            }

            // If there exists log4j.properties in SPARK_HOME/conf directory, Spark from 2.3.*
            // to 2.4.0 hang in E2E test. The reverse behavior is true for Spark 2.4.1; if
            // there does not exist log4j.properties, the tests hang.
            // Note that the hang happens in JVM when it tries to append a console logger (log4j).
            // The solution is to use custom log configuration that appends NullLogger, which
            // works across all Spark versions.
            string resourceUri = new Uri(TestEnvironment.ResourceDirectory).AbsoluteUri;
            string logOption = $"--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=" +
                $"{resourceUri}/log4j.properties";

            args = $"{logOption} {classArg} --master local {jar} debug";
        }

        private string GetJarPrefix(string sparkHome)
        {
            Version sparkVersion = SparkSettings.Version;
            return $"microsoft-spark-{sparkVersion.Major}.{sparkVersion.Minor}.x";
        }
    }

    [CollectionDefinition("Spark E2E Tests")]
    public class SparkCollection : ICollectionFixture<SparkFixture>
    {
        // This class has no code, and is never created. Its purpose is simply
        // to be the place to apply [CollectionDefinition] and all the
        // ICollectionFixture<> interfaces.
    }
}
