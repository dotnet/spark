// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.InteropServices;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// Discover the spark settings
    /// </summary>
    internal class SparkSettings
    {
        private static string SPARK_HOME_ENV_KEY = "SPARK_HOME";

        /// <summary>
        /// The spark home
        /// This would be empty string when failed to findout.
        /// </summary>
        public string SPARK_HOME {get; private set;}

        /// <summary>
        /// The spark submit file
        /// </summary>
        public string SPARK_SUBMIT {get;private set;}


        public Version Version { get; private set; }

        private static string sparksubmitcmd = 
            RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? "spark-submit.cmd" : "spark-submit";

        /// <summary>
        /// Init the spark settings
        /// </summary>
        public SparkSettings()
        {
            find_spark_home();
            this.SPARK_SUBMIT = locateSparkSubmit(SPARK_HOME);
            InitVersion();
        }

        /// <summary>
        /// Locate SPARK_HOME path
        /// </summary>
        /// <returns>Return empty string when failed to find out.</returns>
        private void find_spark_home()
        {
            SPARK_HOME = Environment.GetEnvironmentVariable(SPARK_HOME_ENV_KEY);
            if (string.IsNullOrWhiteSpace(SPARK_HOME) == false)
            {
                return;
            }
            foreach (var possiblehome in possible_spark_home())
            {
                if (isSparkHome(possiblehome))
                {
                    SPARK_HOME = possiblehome;
                    Environment.SetEnvironmentVariable(SPARK_HOME_ENV_KEY, SPARK_HOME);
                    return;
                }
            }
        }

        private IEnumerable<string> possible_spark_home()
        {
            var pathSeperator = ':';
            var envPath = "PATH";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                pathSeperator = ';';
                envPath = "Path";
            }
            // try Env Pathes if we could locate spark
            foreach (var path in Environment.GetEnvironmentVariable(envPath).Split(pathSeperator))
            {
                yield return path;
                yield return Path.GetFullPath(Path.Combine(path, ".."));
            }
        }

        /// <summary>
        /// find out the existing spark-submit file
        /// </summary>
        private string locateSparkSubmit(string sparkhome)
        {
            var fn = Path.Combine(sparkhome, "bin", sparksubmitcmd);
            return File.Exists(fn)
                ? fn : string.Empty;
        }

        private string locateJarFolder(string sparkhome)
        {
            var possible = new string[] { "jars", "assembly" };
            foreach (var tryfolder in possible)
            {
                var folder = Path.Combine(sparkhome, tryfolder);
                if (Directory.Exists(folder))
                {
                    return folder;
                }
            }
            return string.Empty;
        }

        /// <summary>
        /// Check if it is a reasonable SPARK_HOME
        /// </summary>
        private bool isSparkHome(string path)
        {
            return (locateSparkSubmit(path) != string.Empty) &&
                (locateJarFolder(path) != string.Empty);
        }

        /// <summary>
        /// After init spark home, try to find out spark version
        /// </summary>
        private void InitVersion()
        {
            var releasefile = $"{SPARK_HOME}{Path.DirectorySeparatorChar}RELEASE";
            var sparkversionstr = string.Empty;
            if (File.Exists(releasefile))
            {
                // First line of the RELEASE file under SPARK_HOME will be something similar to:
                // Spark 2.3.2 built for Hadoop 2.7.3
                var firstLine = File.ReadLines(releasefile).FirstOrDefault();
                var columns = firstLine?.Split(' ');
                if (columns.Length >= 1)
                {
                    sparkversionstr = columns[1];
                }
            }
            if (string.IsNullOrWhiteSpace(sparkversionstr))
            {
                this.Version = new Version();
            }
            else
            {
                this.Version = new Version(sparkversionstr);
            }
        }

    }
}