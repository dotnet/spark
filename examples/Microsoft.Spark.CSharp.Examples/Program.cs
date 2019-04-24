// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Microsoft.Spark.Examples
{
    public class Program
    {
        public static void Main(string[] args)
        {
            string rootNamespace = MethodBase.GetCurrentMethod().DeclaringType.Namespace;

            // Find all types in the current assembly that implement IExample
            // and is in or starts with rootNamespace. Track the fully qualified
            // name of the type after the rootNamespace.
            IEnumerable<string> examples = Assembly.GetExecutingAssembly().GetTypes()
                .Where(t =>
                    typeof(IExample).IsAssignableFrom(t) &&
                    !t.IsInterface &&
                    !t.IsAbstract &&
                    t.Namespace.StartsWith(rootNamespace) &&
                    ((t.Namespace.Length == rootNamespace.Length) ||
                     (t.Namespace[rootNamespace.Length] == '.')))
                .Select(t => t.FullName.Substring(rootNamespace.Length + 1));

            if ((args.Length == 0) || !TryFindExample(examples, args[0], out string exampleName))
            {
                PrintUsage(examples);
                return;
            }

            string[] exampleArgs = args.Skip(1).ToArray();
            Type type = Assembly.GetExecutingAssembly().GetType($"{rootNamespace}.{exampleName}");
            object instance = Activator.CreateInstance(type);
            MethodInfo method = type.GetMethod("Run");
            method.Invoke(instance, new object[] { exampleArgs });
        }

        private static void PrintUsage(IEnumerable<string> examples)
        {
            string assemblyName = Assembly.GetExecutingAssembly().GetName().Name;
            Console.WriteLine($"Usage: {assemblyName} <example> <example args>");
            if (examples.Any())
            {
                Console.WriteLine("Examples:\n\t*" + string.Join("\n\t*", examples));
            }
            Console.WriteLine($"\n'{assemblyName} <example>' to get the usage info of each example.");
        }

        private static bool TryFindExample(IEnumerable<string> examples, string search,
            out string found)
        {
            found = examples.FirstOrDefault(e =>
                e.Equals(search, StringComparison.InvariantCultureIgnoreCase));
            return !string.IsNullOrWhiteSpace(found);
        }
    }
}
