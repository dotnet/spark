/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.deploy.dotnet

import java.io.File
import java.net.URI
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{FileSystems, Files, Paths}
import java.util.Locale
import java.util.concurrent.{Semaphore, TimeUnit}

import org.apache.commons.io.FilenameUtils
import org.apache.commons.io.output.TeeOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark
import org.apache.spark.api.dotnet.DotnetBackend
import org.apache.spark.deploy.{PythonRunner, SparkHadoopUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.dotnet.Dotnet.{
  DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK,
  ERROR_BUFFER_SIZE, ERROR_REDIRECITON_ENABLED
}
import org.apache.spark.util.dotnet.{Utils => DotnetUtils}
import org.apache.spark.util.{CircularBuffer, RedirectThread, Utils}
import org.apache.spark.{SecurityManager, SparkConf, SparkUserAppException}

import scala.collection.JavaConverters._
import scala.io.StdIn
import scala.util.Try

/**
 * DotnetRunner class used to launch Spark .NET applications using spark-submit.
 * It executes .NET application as a subprocess and then has it connect back to
 * the JVM to access system properties etc.
 */
object DotnetRunner extends Logging {
  private val DEBUG_PORT = 5567
  private val supportedSparkMajorMinorVersionPrefix = "3.3"
  private val supportedSparkVersions = Set[String]("3.3.0", "3.3.1", "3.3.2", "3.3.3", "3.3.4")

  val SPARK_VERSION = DotnetUtils.normalizeSparkVersion(spark.SPARK_VERSION)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new IllegalArgumentException("At least one argument is expected.")
    }

    DotnetUtils.validateSparkVersions(
      sys.props
        .getOrElse(
          DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK.key,
          DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK.defaultValue.get.toString)
        .toBoolean,
      spark.SPARK_VERSION,
      SPARK_VERSION,
      supportedSparkMajorMinorVersionPrefix,
      supportedSparkVersions)

    val settings = initializeSettings(args)

    // Determines if this needs to be run in debug mode.
    // In debug mode this runner will not launch a .NET process.
    val runInDebugMode = settings._1
    @volatile var dotnetBackendPortNumber = settings._2
    var dotnetExecutable = ""
    var otherArgs: Array[String] = null

    if (!runInDebugMode) {
      if (args(0).toLowerCase(Locale.ROOT).endsWith(".zip")) {
        var zipFileName = args(0)
        val zipFileUri = Try(new URI(zipFileName)).getOrElse(new File(zipFileName).toURI)
        val workingDir = new File("").getAbsoluteFile
        val driverDir = new File(workingDir, FilenameUtils.getBaseName(zipFileUri.getPath()))

        // Standalone cluster mode where .NET application is remotely located.
        if (zipFileUri.getScheme() != "file") {
          zipFileName = downloadDriverFile(zipFileName, workingDir.getAbsolutePath).getName
        }

        logInfo(s"Unzipping .NET driver $zipFileName to $driverDir")
        DotnetUtils.unzip(new File(zipFileName), driverDir)

        // Reuse windows-specific formatting in PythonRunner.
        dotnetExecutable = PythonRunner.formatPath(resolveDotnetExecutable(driverDir, args(1)))
        otherArgs = args.slice(2, args.length)
      } else {
        // Reuse windows-specific formatting in PythonRunner.
        dotnetExecutable = PythonRunner.formatPath(args(0))
        otherArgs = args.slice(1, args.length)
      }
    } else {
      otherArgs = args.slice(1, args.length)
    }

    val processParameters = new java.util.ArrayList[String]
    processParameters.add(dotnetExecutable)
    otherArgs.foreach(arg => processParameters.add(arg))

    logInfo(s"Starting DotnetBackend with $dotnetExecutable.")

    // Time to wait for DotnetBackend to initialize in seconds.
    val backendTimeout = sys.env.getOrElse("DOTNETBACKEND_TIMEOUT", "120").toInt

    // Launch a DotnetBackend server for the .NET process to connect to; this will let it see our
    // Java system properties etc.
    val dotnetBackend = new DotnetBackend()
    val initialized = new Semaphore(0)
    val dotnetBackendThread = new Thread("DotnetBackend") {
      override def run() {
        // need to get back dotnetBackendPortNumber because if the value passed to init is 0
        // the port number is dynamically assigned in the backend
        dotnetBackendPortNumber = dotnetBackend.init(dotnetBackendPortNumber)
        logInfo(s"Port number used by DotnetBackend is $dotnetBackendPortNumber")
        initialized.release()
        dotnetBackend.run()
      }
    }

    dotnetBackendThread.start()

    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      if (!runInDebugMode) {
        var returnCode = -1
        var process: Process = null
        val enableLogRedirection: Boolean = sys.props
          .getOrElse(
            ERROR_REDIRECITON_ENABLED.key,
            ERROR_REDIRECITON_ENABLED.defaultValue.get.toString).toBoolean
        val stderrBuffer: Option[CircularBuffer] = Option(enableLogRedirection).collect {
          case true => new CircularBuffer(
            sys.props.getOrElse(
              ERROR_BUFFER_SIZE.key,
              ERROR_BUFFER_SIZE.defaultValue.get.toString).toInt)
        }

        try {
          val builder = new ProcessBuilder(processParameters)
          val env = builder.environment()
          env.put("DOTNETBACKEND_PORT", dotnetBackendPortNumber.toString)

          for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
            env.put(key, value)
            logInfo(s"Adding key=$key and value=$value to environment")
          }
          builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
          process = builder.start()

          // Redirect stdin of JVM process to stdin of .NET process.
          new RedirectThread(System.in, process.getOutputStream, "redirect JVM input").start()
          // Redirect stdout and stderr of .NET process to System.out and to buffer
          // if log direction is enabled. If not, redirect only to System.out.
          new RedirectThread(
            process.getInputStream,
            stderrBuffer match {
              case Some(buffer) => new TeeOutputStream(System.out, buffer)
              case _ => System.out
            },
            "redirect .NET stdout and stderr").start()

          process.waitFor()
        } catch {
          case t: Throwable =>
            logThrowable(t)
        } finally {
          returnCode = closeDotnetProcess(process)
          closeBackend(dotnetBackend)
        }
        if (returnCode != 0) {
          if (stderrBuffer.isDefined) {
            throw new DotNetUserAppException(returnCode, Some(stderrBuffer.get.toString))
          } else {
            throw new SparkUserAppException(returnCode)
          }
        } else {
          logInfo(s".NET application exited successfully")
        }
        // TODO: The following is causing the following error:
        // INFO ApplicationMaster: Final app status: FAILED, exitCode: 16,
        // (reason: Shutdown hook called before final status was reported.)
        // DotnetUtils.exit(returnCode)
      } else {
        // scalastyle:off println
        println("***********************************************************************")
        println("* .NET Backend running debug mode. Press enter to exit *")
        println("***********************************************************************")
        // scalastyle:on println

        StdIn.readLine()
        closeBackend(dotnetBackend)
        DotnetUtils.exit(0)
      }
    } else {
      logError(s"DotnetBackend did not initialize in $backendTimeout seconds")
      DotnetUtils.exit(-1)
    }
  }

  // When the executable is downloaded as part of zip file, check if the file exists
  // after zip file is unzipped under the given dir. Once it is found, change the
  // permission to executable (only for Unix systems, since the zip file may have been
  // created under Windows. Finally, the absolute path for the executable is returned.
  private def resolveDotnetExecutable(dir: File, dotnetExecutable: String): String = {
    val path = Paths.get(dir.getAbsolutePath, dotnetExecutable)
    val resolvedExecutable = if (Files.isRegularFile(path)) {
      path.toAbsolutePath.toString
    } else {
      Files
        .walk(FileSystems.getDefault.getPath(dir.getAbsolutePath))
        .iterator()
        .asScala
        .find(path => Files.isRegularFile(path) && path.getFileName.toString == dotnetExecutable) match {
        case Some(path) => path.toAbsolutePath.toString
        case None =>
          throw new IllegalArgumentException(
            s"Failed to find $dotnetExecutable under ${dir.getAbsolutePath}")
      }
    }

    if (DotnetUtils.supportPosix) {
      Files.setPosixFilePermissions(
        Paths.get(resolvedExecutable),
        PosixFilePermissions.fromString("rwxr-xr-x"))
    }

    resolvedExecutable
  }

  /**
   * Download HDFS file into the supplied directory and return its local path.
   * Will throw an exception if there are errors during downloading.
   */
  private def downloadDriverFile(hdfsFilePath: String, driverDir: String): File = {
    val sparkConf = new SparkConf()
    val filePath = new Path(hdfsFilePath)

    val hadoopConf = SparkHadoopUtil.get.newConfiguration(sparkConf)
    val jarFileName = filePath.getName
    val localFile = new File(driverDir, jarFileName)

    if (!localFile.exists()) { // May already exist if running multiple workers on one node
      logInfo(s"Copying user file $filePath to $driverDir")
      Utils.fetchFile(
        hdfsFilePath,
        new File(driverDir),
        sparkConf,
        hadoopConf,
        System.currentTimeMillis(),
        useCache = false)
    }

    if (!localFile.exists()) {
      throw new Exception(s"Did not see expected $jarFileName in $driverDir")
    }

    localFile
  }

  private def closeBackend(dotnetBackend: DotnetBackend): Unit = {
    logInfo("Closing DotnetBackend")
    dotnetBackend.close()
  }

  private def closeDotnetProcess(dotnetProcess: Process): Int = {
    if (dotnetProcess == null) {
      return -1
    } else if (!dotnetProcess.isAlive) {
      return dotnetProcess.exitValue()
    }

    // Try to (gracefully on Linux) kill the process and resort to force if interrupted
    var returnCode = -1
    logInfo("Closing .NET process")
    try {
      dotnetProcess.destroy()
      returnCode = dotnetProcess.waitFor()
    } catch {
      case _: InterruptedException =>
        logInfo(
          "Thread interrupted while waiting for graceful close. Forcefully closing .NET process")
        returnCode = dotnetProcess.destroyForcibly().waitFor()
      case t: Throwable =>
        logThrowable(t)
    }

    returnCode
  }

  private def initializeSettings(args: Array[String]): (Boolean, Int) = {
    val runInDebugMode = (args.length == 1 || args.length == 2) && args(0).equalsIgnoreCase(
      "debug")
    var portNumber = 0
    if (runInDebugMode) {
      if (args.length == 1) {
        portNumber = DEBUG_PORT
      } else if (args.length == 2) {
        portNumber = Integer.parseInt(args(1))
      }
    }

    (runInDebugMode, portNumber)
  }

  private def logThrowable(throwable: Throwable): Unit =
    logError(s"${throwable.getMessage} \n ${throwable.getStackTrace.mkString("\n")}")
}
