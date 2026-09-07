/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.api.dotnet

import java.net.URI
import java.nio.file.{Files, Path}

import org.apache.spark.SparkContext
import org.apache.spark.deploy.dotnet.DotnetRunner
import org.apache.spark.sql.execution.python.PythonUDFRunner
import org.junit.Assert.{assertEquals, assertThrows, assertTrue}
import org.junit.Test

@Test
class SQLUtilsTest {

  private val EmptyFileSha256 =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

  @Test
  def shouldReturnRuntimeArtifactIdentityInFixedOrder(): Unit = {
    withTemporaryDirectory { directory =>
      val files = Seq(
        "dotnet-runner.jar",
        "sql-utils.jar",
        "spark-sql.jar",
        "spark-core.jar").map(name => Files.createFile(directory.resolve(name)).toRealPath())
      val locations = Map[Class[_], URI](
        DotnetRunner.getClass -> files(0).toUri,
        SQLUtils.getClass -> files(1).toUri,
        classOf[PythonUDFRunner] -> files(2).toUri,
        classOf[SparkContext] -> files(3).toUri)

      val actual = RuntimeArtifactIdentity.capture(clazz => locations(clazz))
      val expected = Seq(
        "DotnetRunner", DotnetRunner.getClass.getName,
        files(0).toString, "0", EmptyFileSha256,
        "SQLUtils", SQLUtils.getClass.getName,
        files(1).toString, "0", EmptyFileSha256,
        "PythonUDFRunner", classOf[PythonUDFRunner].getName,
        files(2).toString, "0", EmptyFileSha256,
        "SparkContext", classOf[SparkContext].getName,
        files(3).toString, "0", EmptyFileSha256)

      assertEquals(20, actual.length)
      assertEquals(expected, actual.toSeq)
    }
  }

  @Test
  def shouldRejectDuplicateLabels(): Unit = {
    val artifacts = Seq(
      RuntimeArtifactIdentity.Artifact("duplicate", classOf[SparkContext]),
      RuntimeArtifactIdentity.Artifact("duplicate", classOf[PythonUDFRunner]))

    val exception = assertThrows(
      classOf[IllegalStateException],
      () => RuntimeArtifactIdentity.capture(artifacts, _ => new URI("file:///unused")))

    assertTrue(exception.getMessage.contains("Duplicate runtime artifact label"))
  }

  @Test
  def shouldRejectMissingCodeSourceLocation(): Unit = {
    val exception = assertThrows(
      classOf[IllegalStateException],
      () => RuntimeArtifactIdentity.capture(_ => null))

    assertTrue(exception.getMessage.contains("Code source location is missing"))
  }

  @Test
  def shouldRejectNonFileCodeSourceLocation(): Unit = {
    val exception = assertThrows(
      classOf[IllegalStateException],
      () => RuntimeArtifactIdentity.capture(_ => new URI("https://example.invalid/artifact.jar")))

    assertTrue(exception.getMessage.contains("non-file code source"))
  }

  @Test
  def shouldRejectDirectoryCodeSourceLocation(): Unit = {
    withTemporaryDirectory { directory =>
      val exception = assertThrows(
        classOf[IllegalStateException],
        () => RuntimeArtifactIdentity.capture(_ => directory.toUri))

      assertTrue(exception.getMessage.contains("not a regular file"))
    }
  }

  private def withTemporaryDirectory(test: Path => Unit): Unit = {
    val directory = Files.createTempDirectory("spark40-runtime-identity-")
    try {
      test(directory)
    } finally {
      val children = Files.list(directory)
      try {
        children.forEach(path => Files.deleteIfExists(path))
      } finally {
        children.close()
      }
      Files.deleteIfExists(directory)
    }
  }
}
