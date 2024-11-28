/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.util.dotnet

import java.io._
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFilePermission._
import java.nio.file.{FileSystems, Files}
import java.util.{Timer, TimerTask}
import org.apache.spark.SparkConf
import org.apache.spark.SecurityManager
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.Utils
import java.io.File
import java.lang.NoSuchMethodException
import java.lang.reflect.InvocationTargetException
import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipArchiveOutputStream, ZipFile}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.dotnet.Dotnet.DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK

import scala.collection.JavaConverters._
import scala.collection.Set

/**
 * Utility methods.
 */
object Utils extends Logging {
  private val posixFilePermissions = Array(
    OWNER_READ,
    OWNER_WRITE,
    OWNER_EXECUTE,
    GROUP_READ,
    GROUP_WRITE,
    GROUP_EXECUTE,
    OTHERS_READ,
    OTHERS_WRITE,
    OTHERS_EXECUTE)

  val supportPosix: Boolean =
    FileSystems.getDefault.supportedFileAttributeViews().contains("posix")

    /**
     * Provides a backward-compatible implementation of the `fetchFile` method 
     * from Apache Spark's `org.apache.spark.util.Utils` class.
     * 
     * This method handles differences in method signatures between Spark versions,
     * specifically the inclusion or absence of a `SecurityManager` parameter. It uses
     * reflection to dynamically resolve and invoke the correct version of `fetchFile`.
     *
     * @param url The source URL of the file to be fetched.
     * @param targetDir The directory where the fetched file will be saved.
     * @param conf The Spark configuration object used to determine runtime settings.
     * @param hadoopConf Hadoop configuration settings for file access.
     * @param timestamp A timestamp indicating the cache validity of the fetched file.
     * @param useCache Whether to use Spark's caching mechanism to reuse previously downloaded files.
     * @param shouldUntar Whether to untar the downloaded file if it is a tarball. Defaults to `true`.
     * 
     * @return A `File` object pointing to the fetched and stored file.
     * 
     * @throws IllegalArgumentException If neither method signature is found.
     * @throws Throwable If an error occurs during reflection or method invocation.
     *
     * Note:
     * - This method was introduced as a fix for DataBricks-specific file copying issues 
     *   and was referenced in PR #1048.
     * - Reflection is used to ensure compatibility across Spark environments.
     */
    def fetchFileWithbackwardCompatibility(
                                      url: String,
                                      targetDir: File,
                                      conf: SparkConf,
                                      hadoopConf: Configuration,
                                      timestamp: Long,
                                      useCache: Boolean,
                                      shouldUntar: Boolean = true): File = {

        val signatureWithSecurityManager = Array(
            classOf[String],
            classOf[File],
            classOf[SparkConf],
            classOf[SecurityManager],
            classOf[Configuration],
            java.lang.Long.TYPE,
            java.lang.Boolean.TYPE,
            java.lang.Boolean.TYPE
        )

        val signatureWithoutSecurityManager = Array(
            classOf[String],
            classOf[File],
            classOf[SparkConf],
            classOf[Configuration],
            classOf[Long],
            classOf[Boolean],
            classOf[Boolean]
        )

        val utilsClass = Class.forName("org.apache.spark.util.Utils$")
        val utilsObject = utilsClass.getField("MODULE$").get(null)

        val (needSecurityManagerArg, method) = {
            try {
                (true, utilsClass.getMethod("fetchFile", signatureWithSecurityManager: _*))
            } catch {
                case _: NoSuchMethodException =>
                    (false, utilsClass.getMethod("fetchFile", signatureWithoutSecurityManager: _*))
            }
        }

        val args: Seq[Any] =
            Seq(
                url,
                targetDir,
                conf
            ) ++ (if (needSecurityManagerArg) Seq(null) else Nil) ++ Seq(
                hadoopConf,
                timestamp,
                useCache,
                shouldUntar)

        // Unwrap InvocationTargetException to preserve exception in case of errors:
        try {
            method.invoke(utilsObject, args.map(_.asInstanceOf[Object]): _*).asInstanceOf[File]
        } catch {
            case e: InvocationTargetException =>
                throw e.getCause()
        }
    }

  /**
   * Compress all files under given directory into one zip file and drop it to the target directory
   *
   * @param sourceDir source directory to zip
   * @param targetZipFile target zip file
   */
  def zip(sourceDir: File, targetZipFile: File): Unit = {
    var fos: FileOutputStream = null
    var zos: ZipArchiveOutputStream = null
    try {
      fos = new FileOutputStream(targetZipFile)
      zos = new ZipArchiveOutputStream(fos)

      val sourcePath = sourceDir.toPath
      FileUtils.listFiles(sourceDir, null, true).asScala.foreach { file =>
        var in: FileInputStream = null
        try {
          val path = file.toPath
          val entry = new ZipArchiveEntry(sourcePath.relativize(path).toString)
          if (supportPosix) {
            entry.setUnixMode(
              permissionsToMode(Files.getPosixFilePermissions(path).asScala)
                | (if (entry.getName.endsWith(".exe")) 0x1ED else 0x1A4))
          } else if (entry.getName.endsWith(".exe")) {
            entry.setUnixMode(0x1ED) // 755
          } else {
            entry.setUnixMode(0x1A4) // 644
          }
          zos.putArchiveEntry(entry)

          in = new FileInputStream(file)
          IOUtils.copy(in, zos)
          zos.closeArchiveEntry()
        } finally {
          IOUtils.closeQuietly(in)
        }
      }
    } finally {
      IOUtils.closeQuietly(zos)
      IOUtils.closeQuietly(fos)
    }
  }

  /**
   * Unzip a file to the given directory
   *
   * @param file file to be unzipped
   * @param targetDir target directory
   */
  def unzip(file: File, targetDir: File): Unit = {
    var zipFile: ZipFile = null
    try {
      targetDir.mkdirs()
      zipFile = new ZipFile(file)
      zipFile.getEntries.asScala.foreach { entry =>
        val targetFile = new File(targetDir, entry.getName)

        if (targetFile.exists()) {
          logWarning(
            s"Target file/directory $targetFile already exists. Skip it for now. " +
              s"Make sure this is expected.")
        } else {
          if (entry.isDirectory) {
            targetFile.mkdirs()
          } else {
            targetFile.getParentFile.mkdirs()
            val input = zipFile.getInputStream(entry)
            val output = new FileOutputStream(targetFile)
            IOUtils.copy(input, output)
            IOUtils.closeQuietly(input)
            IOUtils.closeQuietly(output)
            if (supportPosix) {
              val permissions = modeToPermissions(entry.getUnixMode)
              // When run in Unix system, permissions will be empty, thus skip
              // setting the empty permissions (which will empty the previous permissions).
              if (permissions.nonEmpty) {
                Files.setPosixFilePermissions(targetFile.toPath, permissions.asJava)
              }
            }
          }
        }
      }
    } catch {
      case e: Exception => logError("exception caught during decompression:" + e)
    } finally {
      ZipFile.closeQuietly(zipFile)
    }
  }

  /**
   * Exits the JVM, trying to do it nicely, otherwise doing it nastily.
   *
   * @param status  the exit status, zero for OK, non-zero for error
   * @param maxDelayMillis  the maximum delay in milliseconds
   */
  def exit(status: Int, maxDelayMillis: Long) {
    try {
      logInfo(s"Utils.exit() with status: $status, maxDelayMillis: $maxDelayMillis")

      // setup a timer, so if nice exit fails, the nasty exit happens
      val timer = new Timer()
      timer.schedule(new TimerTask() {

        override def run() {
          Runtime.getRuntime.halt(status)
        }
      }, maxDelayMillis)
      // try to exit nicely
      System.exit(status);
    } catch {
      // exit nastily if we have a problem
      case _: Throwable => Runtime.getRuntime.halt(status)
    } finally {
      // should never get here
      Runtime.getRuntime.halt(status)
    }
  }

  /**
   * Exits the JVM, trying to do it nicely, wait 1 second
   *
   * @param status  the exit status, zero for OK, non-zero for error
   */
  def exit(status: Int): Unit = {
    exit(status, 1000)
  }

  /**
   * Normalize the Spark version by taking the first three numbers.
   * For example:
   * x.y.z => x.y.z
   * x.y.z.xxx.yyy => x.y.z
   * x.y => x.y
   *
   * @param version the Spark version to normalize
   * @return Normalized Spark version.
   */
  def normalizeSparkVersion(version: String): String = {
    version
      .split('.')
      .take(3)
      .zipWithIndex
      .map({
        case (element, index) => {
          index match {
            case 2 => element.split("\\D+").lift(0).getOrElse("")
            case _ => element
          }
        }
      })
      .mkString(".")
  }

  /**
   * Validates the normalized spark version by verifying:
   *   - Spark version starts with sparkMajorMinorVersionPrefix.
   *   - If ignoreSparkPatchVersion is
   *     - true: valid
   *     - false: check if the spark version is in supportedSparkVersions.
   * @param ignoreSparkPatchVersion Ignore spark patch version.
   * @param sparkVersion The spark version.
   * @param normalizedSparkVersion: The normalized spark version.
   * @param supportedSparkMajorMinorVersionPrefix The spark major and minor version to validate against.
   * @param supportedSparkVersions The set of supported spark versions.
   */
  def validateSparkVersions(
      ignoreSparkPatchVersion: Boolean,
      sparkVersion: String,
      normalizedSparkVersion: String,
      supportedSparkMajorMinorVersionPrefix: String,
      supportedSparkVersions: Set[String]): Unit = {
    if (!normalizedSparkVersion.startsWith(s"$supportedSparkMajorMinorVersionPrefix.")) {
      throw new IllegalArgumentException(
        s"Unsupported spark version used: '$sparkVersion'. " +
          s"Normalized spark version used: '$normalizedSparkVersion'. " +
          s"Supported spark major.minor version: '$supportedSparkMajorMinorVersionPrefix'.")
    } else if (ignoreSparkPatchVersion) {
      logWarning(
        s"Ignoring spark patch version. Spark version used: '$sparkVersion'. " +
          s"Normalized spark version used: '$normalizedSparkVersion'. " +
          s"Spark major.minor prefix used: '$supportedSparkMajorMinorVersionPrefix'.")
    } else if (!supportedSparkVersions(normalizedSparkVersion)) {
      val supportedVersions = supportedSparkVersions.toSeq.sorted.mkString(", ")
      throw new IllegalArgumentException(
        s"Unsupported spark version used: '$sparkVersion'. " +
          s"Normalized spark version used: '$normalizedSparkVersion'. " +
          s"Supported versions: '$supportedVersions'." +
          "Patch version can be ignored, use setting 'spark.dotnet.ignoreSparkPatchVersionCheck'"  )
    }
  }

  private[spark] def listZipFileEntries(file: File): Array[String] = {
    var zipFile: ZipFile = null
    try {
      zipFile = new ZipFile(file)
      zipFile.getEntries.asScala.map(_.getName).toArray
    } finally {
      ZipFile.closeQuietly(zipFile)
    }
  }

  private[this] def permissionsToMode(permissions: Set[PosixFilePermission]): Int = {
    posixFilePermissions.foldLeft(0) { (mode, perm) =>
      (mode << 1) | (if (permissions.contains(perm)) 1 else 0)
    }
  }

  private[this] def modeToPermissions(mode: Int): Set[PosixFilePermission] = {
    posixFilePermissions.zipWithIndex
      .filter { case (_, i) => (mode & (0x100 >>> i)) != 0 }
      .map(_._1)
      .toSet
  }
}
