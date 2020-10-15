$version = $args[0]
$worker_dir = $args[1]
$output_dir = $args[2]

$worker_version_dir = "Microsoft.Spark.Worker-$version"

$frameworks = Get-ChildItem -Directory $worker_dir
foreach ($framework in $frameworks)
{
    $runtimes = Get-ChildItem -Directory $framework.FullName
    foreach ($runtime in $runtimes)
    {
        New-Item $worker_version_dir -ItemType Directory
        Copy-Item "$($runtime.FullName)\*" -Destination $worker_version_dir -Recurse
        $filename = "Microsoft.Spark.Worker.$framework.$runtime-$version"

        # Generate additional tar.gz worker files only for linux-x64.
        if ($runtime.Name.ToLower().Equals("linux-x64"))
        {
            tar czf "$output_dir/$filename.tar.gz" $worker_version_dir --force-local
        }
        
        Compress-Archive -DestinationPath "$output_dir/$filename.zip" -Path $worker_version_dir -CompressionLevel Optimal

        Remove-Item -Path $worker_version_dir -Recurse -Force
    }
}
