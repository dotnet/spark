// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Microsoft.Spark.Worker.Utils
{
    /// <summary>
    /// FilePrinter is responsible for getting printable string for the filesystem.
    /// </summary>
    internal static class FilePrinter
    {
        /// <summary>
        /// Returns the string that displays the files from the current working directory.
        /// </summary>
        /// <returns>String that captures files info.</returns>
        public static string GetString()
        {
            string dir = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            string[] files = Directory.EnumerateFiles(dir).Select(Path.GetFileName).ToArray();
            int longest = files.Max(f => f.Length);
            int count = 0;
            var message = new StringBuilder();

            message.Append($"Dir: {dir}{Environment.NewLine}");
            message.Append($"Files:{Environment.NewLine}");

            foreach (string file in files)
            {
                switch (count++ % 2)
                {
                    case 0:
                        message.Append("   " + file.PadRight(longest + 2));
                        break;
                    default:
                        message.AppendLine(file);
                        break;
                }
            }

            return message.ToString();
        }
    }
}
