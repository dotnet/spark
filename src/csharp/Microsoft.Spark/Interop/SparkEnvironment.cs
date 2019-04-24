// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System.Runtime.CompilerServices;
using Microsoft.Spark.Interop.Ipc;
using Microsoft.Spark.Services;

// TODO: Move the followings to the AssemblyInfo.cs.
[assembly: InternalsVisibleTo("Microsoft.Spark.UnitTest, PublicKey=002400000480000094000000060200000024000052534131000400000100010015c01ae1f50e8cc09ba9eac9147cf8fd9fce2cfe9f8dce4f7301c4132ca9fb50ce8cbf1df4dc18dd4d210e4345c744ecb3365ed327efdbc52603faa5e21daa11234c8c4a73e51f03bf192544581ebe107adee3a34928e39d04e524a9ce729d5090bfd7dad9d10c722c0def9ccc08ff0a03790e48bcd1f9b6c476063e1966a1c4")]
[assembly: InternalsVisibleTo("Microsoft.Spark.Worker, PublicKey=00240000048000009400000006020000002400005253413100040000010001004b86c4cb78549b34bab61a3b1800e23bfeb5b3ec390074041536a7e3cbd97f5f04cf0f857155a8928eaa29ebfd11cfbbad3ba70efea7bda3226c6a8d370a4cd303f714486b6ebc225985a638471e6ef571cc92a4613c00b8fa65d61ccee0cbe5f36330c9a01f4183559f1bef24cc2917c6d913e3a541333a1d05d9bed22b38cb")]
[assembly: InternalsVisibleTo("Microsoft.Spark.Worker.UnitTest, PublicKey=002400000480000094000000060200000024000052534131000400000100010015c01ae1f50e8cc09ba9eac9147cf8fd9fce2cfe9f8dce4f7301c4132ca9fb50ce8cbf1df4dc18dd4d210e4345c744ecb3365ed327efdbc52603faa5e21daa11234c8c4a73e51f03bf192544581ebe107adee3a34928e39d04e524a9ce729d5090bfd7dad9d10c722c0def9ccc08ff0a03790e48bcd1f9b6c476063e1966a1c4")]
[assembly: InternalsVisibleTo("Microsoft.Spark.E2ETest, PublicKey=002400000480000094000000060200000024000052534131000400000100010015c01ae1f50e8cc09ba9eac9147cf8fd9fce2cfe9f8dce4f7301c4132ca9fb50ce8cbf1df4dc18dd4d210e4345c744ecb3365ed327efdbc52603faa5e21daa11234c8c4a73e51f03bf192544581ebe107adee3a34928e39d04e524a9ce729d5090bfd7dad9d10c722c0def9ccc08ff0a03790e48bcd1f9b6c476063e1966a1c4")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2, PublicKey=0024000004800000940000000602000000240000525341310004000001000100c547cac37abd99c8db225ef2f6c8a3602f3b3606cc9891605d02baa56104f4cfc0734aa39b93bf7852f7d9266654753cc297e7d2edfe0bac1cdcf9f717241550e0a7b191195b7667bb4f64bcb8e2121380fd1d9d46ad2d92d2d15605093924cceaf74c4861eff62abf69b9291ed0a340e113be11e6a7d3113e92484cf7045cc7")] // For Moq

namespace Microsoft.Spark.Interop
{
    /// <summary>
    /// Contains everything needed to setup an environment for using .NET with Spark.
    /// </summary>
    internal static class SparkEnvironment
    {
        private static IJvmBridge s_jvmBridge;
        internal static IJvmBridge JvmBridge
        {
            get
            {
                if (s_jvmBridge == null)
                {
                    s_jvmBridge = new JvmBridge(ConfigurationService.BackendPortNumber);
                }

                return s_jvmBridge;
            }
            set
            {
                s_jvmBridge = value;
            }
        }

        private static IConfigurationService s_configurationService;
        internal static IConfigurationService ConfigurationService
        {
            get
            {
                return s_configurationService ??
                    (s_configurationService = new ConfigurationService());
            }
            set
            {
                s_configurationService = value;
            }
        }
    }
}
