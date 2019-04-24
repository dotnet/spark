// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

module Microsoft.Spark.Examples.Main

open System
open System.Reflection

let printUsage (examples : string seq) =
    let assemblyName = Assembly.GetExecutingAssembly().GetName().Name
    printfn "Usage: %s <example> <example args>" assemblyName

    if not (Seq.isEmpty examples) then
        printfn "Examples:\n\t*%s" (examples |> String.concat "\n\t*")
    printfn "\n'%s <example>' to get the usage info of each example." assemblyName

let tryFindExample search (examples: string seq) =
    examples |> Seq.tryFind (fun e -> e.Equals(search, StringComparison.InvariantCultureIgnoreCase))

[<EntryPoint>]
let main args =
    let rootNamespace = MethodBase.GetCurrentMethod().DeclaringType.Namespace

    // Find all types in the current assembly that implement IExample
    // and is in or starts with rootNamespace. Track the fully qualified
    // name of the type after the rootNamespace.
    let examples =
        Assembly.GetExecutingAssembly().GetTypes()
            |> Seq.filter (fun t -> 
                typeof<IExample>.IsAssignableFrom(t) &&
                not t.IsInterface &&
                not t.IsAbstract &&
                t.Namespace.StartsWith(rootNamespace) &&
                ((t.Namespace.Length = rootNamespace.Length) ||
                 (t.Namespace.[rootNamespace.Length] = '.')))
            |> Seq.map (fun t -> t.FullName.Substring(rootNamespace.Length + 1))

    match args with
    | [||] ->
        printUsage examples
        1
    | _ ->
        match examples |> tryFindExample (args |> Array.head) with
        | None ->
            printUsage examples
            1
        | Some exampleName ->
            let exampleArgs = args |> Array.skip 1
            let exampleType =
                Assembly.GetExecutingAssembly()
                    .GetType(sprintf "%s.%s"  rootNamespace exampleName)
            let instance = Activator.CreateInstance(exampleType)
            let method = exampleType.GetMethod("Run")
            method.Invoke(instance, [|exampleArgs|]) :?> int