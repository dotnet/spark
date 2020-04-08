@echo off

setlocal

set OutputDir=%1
set OldestCompatibleWorkerVersion=%2
cd %OutputDir%

echo "Download oldest backwards compatible worker release - %OldestCompatibleWorkerVersion%"
curl -k -L -o Microsoft.Spark.Worker.zip https://github.com/dotnet/spark/releases/download/v%OldestCompatibleWorkerVersion%/Microsoft.Spark.Worker.netcoreapp3.1.win-x64-%OldestCompatibleWorkerVersion%.zip
unzip Microsoft.Spark.Worker.zip

endlocal
