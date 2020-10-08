// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Net;
using System.Reflection;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Experimental.Utils
{
    /// <summary>
    /// Gets the <see cref="AssemblyInfo"/> for the "Microsoft.Spark" and "Microsoft.Spark.Worker"
    /// assemblies if they exist within the current execution context of this application domain.
    /// </summary>
    internal class VersionSensor
    {
        private const string MicrosoftSparkAssemblyName = "Microsoft.Spark";
        private const string MicrosoftSparkWorkerAssemblyName = "Microsoft.Spark.Worker";

        private static readonly Lazy<AssemblyInfo> s_microsoftSparkVersionInfo =
            new Lazy<AssemblyInfo>(() => CreateVersionInfo(MicrosoftSparkAssemblyName));

        private static readonly Lazy<AssemblyInfo> s_microsoftSparkWorkerVersionInfo =
            new Lazy<AssemblyInfo>(() => CreateVersionInfo(MicrosoftSparkWorkerAssemblyName));

        internal static AssemblyInfo MicrosoftSparkVersion() => s_microsoftSparkVersionInfo.Value;

        internal static AssemblyInfo MicrosoftSparkWorkerVersion() =>
            s_microsoftSparkWorkerVersionInfo.Value;

        private static AssemblyInfo CreateVersionInfo(string assemblyName)
        {
            Assembly assembly = AppDomain
                .CurrentDomain
                .GetAssemblies()
                .SingleOrDefault(asm => asm.GetName().Name == assemblyName);

            AssemblyName asmName = assembly.GetName();
            return new AssemblyInfo
            {
                AssemblyName = asmName.Name,
                AssemblyVersion = asmName.Version.ToString(),
                HostName = Dns.GetHostName()
            };
        }

        internal class AssemblyInfo
        {
            internal static readonly StructType s_schema = new StructType(
                new StructField[]
                {
                    new StructField("AssemblyName", new StringType(), isNullable: false),
                    new StructField("AssemblyVersion", new StringType(), isNullable: false),
                    new StructField("HostName", new StringType(), isNullable: false)
                });

            internal string AssemblyName { get; set; }
            internal string AssemblyVersion { get; set; }
            internal string HostName { get; set; }

            internal GenericRow ToGenericRow() =>
                new GenericRow(new object[] { AssemblyName, AssemblyVersion, HostName });
        }
    }
}
