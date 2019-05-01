$nuget_dir = $args[0]
$worker_dir = $args[1]
$output_dir = $args[2]

$file = Get-ChildItem $nuget_dir -Filter Microsoft.Spark.*.nupkg | Select-Object -First 1
$version = $file.Basename.Split(".", 3)[2]
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

        if ($runtime.Name.ToLower().Equals("linux-x64"))
        {
            tar czf "$output_dir/$filename.tar.gz" $worker_version_dir
        }
        else
        {
            Compress-Archive -DestinationPath "$output_dir/$filename.zip" -Path $worker_version_dir -CompressionLevel Optimal
        }

        Remove-Item -Path $worker_version_dir -Recurse -Force
    }
}