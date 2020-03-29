@echo off

setlocal

set OutputDir=%1
cd %OutputDir%

echo "Download oldest backwards compatible worker release"
curl -k -L -o Microsoft.Spark.Worker.zip https://github.com/dotnet/spark/releases/download/v0.8.0/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-0.8.0.zip
unzip Microsoft.Spark.Worker.zip

endlocal
