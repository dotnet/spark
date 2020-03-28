@echo off

setlocal

set OutputDir=%1
cd %OutputDir%

echo "Download oldest backwards compatible worker release"
curl -k -L -o Microsoft.Spark.Worker.zip https://github.com/dotnet/spark/releases/download/v0.9.0/Microsoft.Spark.Worker.netcoreapp3.1.linux-x64-0.9.0.zip
unzip Microsoft.Spark.Worker.zip
mkdir -p Microsoft.Spark.Worker
cp -a Microsoft.Spark.Worker-0.9.0/ Microsoft.Spark.Worker/

endlocal