// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using Xunit.Sdk;

namespace Microsoft.Spark.E2ETest
{
    internal static class SparkSettings
    {
        internal static Version Version { get; private set; }
        internal static string SparkHome { get; private set; }

        static SparkSettings()
        {
            InitSparkHome();
            InitVersion();
        }

        private static void InitSparkHome()
        {
            SparkHome = Environment.GetEnvironmentVariable("SPARK_HOME");
            if (SparkHome == null)
            {
                throw new NullException("SPARK_HOME environment variable is not set.");
            }
        }

        private static void InitVersion()
        {
            // First line of the RELEASE file under SPARK_HOME will be something similar to:
            // Spark 2.4.0 built for Hadoop 2.7.3
            string firstLine =
                File.ReadLines($"{SparkHome}{Path.DirectorySeparatorChar}RELEASE").First();

            // Grab "2.4.0" from "Spark 2.4.0 built for Hadoop 2.7.3"
            string versionStr = firstLine.Split(' ')[1];

            // Strip anything below version number.
            // For example, "3.0.0-preview" should become "3.0.0".
            Version = new Version(versionStr.Split('-')[0]);
        }
    }
}
