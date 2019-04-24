@echo off

setlocal

set Build.SourcesDirectory=%1
set Build.ArtifactStagingDirectory=%2
set Build.Configuration=%3

CALL :PublishWorker net461, win-x64
CALL :PublishWorker netcoreapp2.1, win-x64 
CALL :PublishWorker netcoreapp2.1, ubuntu.16.04-x64
CALL :PublishWorker netcoreapp2.1, ubuntu.18.04-x64
EXIT /B %ERRORLEVEL%

:PublishWorker
set Framework=%~1
set Runtime=%~2
mkdir %Build.ArtifactStagingDirectory%\%Framework%\%Runtime%
dotnet publish %Build.SourcesDirectory%\src\csharp\Microsoft.Spark.Worker\Microsoft.Spark.Worker.csproj --configuration %Build.Configuration% --framework %Framework% --runtime %Runtime% --output %Build.ArtifactStagingDirectory%\%Framework%\%Runtime%
EXIT /B 0

endlocal                                            