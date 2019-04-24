// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

module Microsoft.Spark.Examples.Main

open System
open System.Collections.Generic
open System.Linq
open System.Reflection
open System.Runtime.InteropServices

let printUsage (examples : IEnumerable<string>) =
    let assemblyName = Assembly.GetExecutingAssembly().GetName().Name
    printfn "Usage: %s <example> <example args>" assemblyName

    if examples.Any() then
        printfn "Examples:\n\t*%s" (examples |> String.concat "\n\t*")
    printfn "\n'%s <example>' to get the usage info of each example." assemblyName

let tryFindExample (examples: IEnumerable<string>, search: string, [<Out>] found : string byref) =
    found <- examples.FirstOrDefault(fun e ->
        e.Equals(search, StringComparison.InvariantCultureIgnoreCase))
    not (String.IsNullOrWhiteSpace(found))

[<EntryPoint>]
let main args =
    let rootNamespace = MethodBase.GetCurrentMethod().DeclaringType.Namespace

    // Find all types in the current assembly that implement IExample
    // and is in or starts with rootNamespace. Track the fully qualified
    // name of the type after the rootNamespace.
    let examples =
        Assembly.GetExecutingAssembly().GetTypes()
            .Where(fun t -> 
                typeof<IExample>.IsAssignableFrom(t) &&
                not t.IsInterface &&
                not t.IsAbstract &&
                t.Namespace.StartsWith(rootNamespace) &&
                ((t.Namespace.Length = rootNamespace.Length) ||
                 (t.Namespace.[rootNamespace.Length] = '.')))
            .Select(fun t -> t.FullName.Substring(rootNamespace.Length + 1))

    match args with
    | [||] ->
        printUsage(examples)
        1
    | _ ->
        let mutable exampleName = String.Empty
        if not (tryFindExample(examples, args.[0], &exampleName)) then
            printUsage(examples)
            1
        else
            let exampleArgs = args.Skip(1).ToArray()
            let exampleType =
                Assembly.GetExecutingAssembly()
                    .GetType(sprintf "%s.%s"  rootNamespace exampleName)
            let instance = Activator.CreateInstance(exampleType)
            let method = exampleType.GetMethod("Run")
            method.Invoke(instance, [|exampleArgs|]) :?> int
