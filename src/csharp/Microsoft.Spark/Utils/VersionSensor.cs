// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Net;
using System.Reflection;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;

namespace Microsoft.Spark.Utils
{
    internal class VersionSensor
    {
        internal static VersionInfo MicrosoftSparkVersion() => s_microsoftSparkVersionInfo.Value;

        internal static VersionInfo MicrosoftSparkWorkerVersion() =>
            s_microsoftSparkWorkerVersionInfo.Value;

        private static readonly Lazy<VersionInfo> s_microsoftSparkVersionInfo =
            new Lazy<VersionInfo>(() =>
            {
                Assembly assembly =
                    AppDomain.CurrentDomain
                        .GetAssemblies()
                        .SingleOrDefault(asm => asm.GetName().Name == "Microsoft.Spark");

                return CreateVersionInfo(assembly);
            });

        private static readonly Lazy<VersionInfo> s_microsoftSparkWorkerVersionInfo =
            new Lazy<VersionInfo>(() =>
            {
                Assembly assembly =
                    AppDomain.CurrentDomain
                        .GetAssemblies()
                        .SingleOrDefault(asm => asm.GetName().Name == "Microsoft.Spark.Worker");

                return CreateVersionInfo(assembly);
            });

        private static VersionInfo CreateVersionInfo(Assembly assembly)
        {
            AssemblyName asmName = assembly.GetName();
            return new VersionInfo
            {
                AssemblyName = asmName.Name,
                AssemblyVersion = asmName.Version.ToString(),
                HostName = Dns.GetHostName()
            };
        }

        internal class VersionInfo
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
