/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.api.dotnet

import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.util.{List => JList, Map => JMap}

import org.apache.spark.SparkContext
import org.apache.spark.api.python.{PythonAccumulatorV2, PythonBroadcast, PythonFunction, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.dotnet.DotnetRunner
import org.apache.spark.sql.execution.python.PythonUDFRunner

import scala.util.control.NonFatal

object SQLUtils {

  /**
   * Exposes createPythonFunction to the .NET client to enable registering UDFs.
   */
  def createPythonFunction(
      command: Array[Byte],
      envVars: JMap[String, String],
      pythonIncludes: JList[String],
      pythonExec: String,
      pythonVersion: String,
      broadcastVars: JList[Broadcast[PythonBroadcast]],
      accumulator: PythonAccumulatorV2): PythonFunction = {
    // From 3.4.0 use SimplePythonFunction. https://github.com/apache/spark/commit/18ff15729268def5ee1bdf5dfcb766bd1d699684
    SimplePythonFunction(
      command,
      envVars,
      pythonIncludes,
      pythonExec,
      pythonVersion,
      broadcastVars,
      accumulator)
  }

  /**
   * Returns the identities of the bridge and Spark artifacts that contain the classes used by
   * the Spark 4 SQL UDF path.
   */
  def runtimeArtifactIdentity(): Array[String] = RuntimeArtifactIdentity.capture()
}

private[dotnet] object RuntimeArtifactIdentity {
  final case class Artifact(label: String, binaryClass: Class[_])

  private val RuntimeArtifacts = Seq(
    Artifact("DotnetRunner", DotnetRunner.getClass),
    Artifact("SQLUtils", SQLUtils.getClass),
    Artifact("PythonUDFRunner", classOf[PythonUDFRunner]),
    Artifact("SparkContext", classOf[SparkContext]))
  private val HexDigits = "0123456789abcdef".toCharArray

  def capture(): Array[String] = capture(RuntimeArtifacts, codeSourceLocation)

  private[dotnet] def capture(locationOf: Class[_] => URI): Array[String] =
    capture(RuntimeArtifacts, locationOf)

  private[dotnet] def capture(
      artifacts: Seq[Artifact],
      locationOf: Class[_] => URI): Array[String] = {
    val duplicateLabels = artifacts
      .groupBy(_.label)
      .collect { case (label, entries) if entries.size > 1 => label }
      .toSeq
      .sorted
    if (duplicateLabels.nonEmpty) {
      throw new IllegalStateException(
        s"Duplicate runtime artifact label: ${duplicateLabels.mkString(", ")}")
    }

    artifacts.iterator.flatMap(artifactIdentity(_, locationOf)).toArray
  }

  private def codeSourceLocation(clazz: Class[_]): URI = {
    val protectionDomain = Option(clazz.getProtectionDomain).getOrElse {
      throw new IllegalStateException(
        s"Protection domain is missing for runtime class '${clazz.getName}'.")
    }
    val codeSource = Option(protectionDomain.getCodeSource).getOrElse {
      throw new IllegalStateException(
        s"Code source is missing for runtime class '${clazz.getName}'.")
    }
    val location = Option(codeSource.getLocation).getOrElse {
      throw new IllegalStateException(
        s"Code source location is missing for runtime class '${clazz.getName}'.")
    }
    location.toURI
  }

  private def artifactIdentity(
      artifact: Artifact,
      locationOf: Class[_] => URI): Seq[String] = {
    try {
      val location = Option(locationOf(artifact.binaryClass)).getOrElse {
        throw new IllegalStateException(
          s"Code source location is missing for runtime class " +
            s"'${artifact.binaryClass.getName}'.")
      }
      if (!Option(location.getScheme).exists(_.equalsIgnoreCase("file"))) {
        throw new IllegalStateException(
          s"Runtime class '${artifact.binaryClass.getName}' has a non-file code source: $location")
      }

      val canonicalPath = Paths.get(location).toRealPath()
      if (!Files.isRegularFile(canonicalPath)) {
        throw new IllegalStateException(
          s"Runtime class '${artifact.binaryClass.getName}' code source is not a regular file: " +
            canonicalPath)
      }

      Seq(
        artifact.label,
        artifact.binaryClass.getName,
        canonicalPath.toString,
        Files.size(canonicalPath).toString,
        sha256(canonicalPath))
    } catch {
      case e: IllegalStateException => throw e
      case NonFatal(e) =>
        throw new IllegalStateException(
          s"Failed to inspect runtime artifact '${artifact.label}'.",
          e)
    }
  }

  private def sha256(path: Path): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    val input = Files.newInputStream(path)
    val buffer = new Array[Byte](8192)
    try {
      var count = input.read(buffer)
      while (count >= 0) {
        if (count > 0) {
          digest.update(buffer, 0, count)
        }
        count = input.read(buffer)
      }
    } finally {
      input.close()
    }

    val bytes = digest.digest()
    val characters = new Array[Char](bytes.length * 2)
    bytes.indices.foreach { index =>
      val value = bytes(index) & 0xff
      characters(index * 2) = HexDigits(value >>> 4)
      characters(index * 2 + 1) = HexDigits(value & 0x0f)
    }
    new String(characters)
  }
}
