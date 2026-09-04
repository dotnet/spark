/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.util.dotnet

import org.apache.spark.SparkConf
import org.apache.spark.deploy.dotnet.DotnetRunner
import org.apache.spark.internal.config.dotnet.Dotnet.DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK
import org.junit.Assert.{assertEquals, assertThrows}
import org.junit.Test

@Test
class UtilsTest {

  private val supportedSparkMajorMinorVersionPrefix = "4.0"
  private val supportedSparkVersions =
    Set[String]("4.0.0", "4.0.1", "4.0.2", "4.0.3", "4.0.4")

  @Test
  def shouldUseSpark404RunnerIdentity(): Unit = {
    assertEquals("4.0.4", DotnetRunner.SPARK_VERSION)
  }

  @Test
  def shouldAcceptSupportedVersions(): Unit = {
    supportedSparkVersions.foreach { sparkVersion =>
      Utils.validateSparkVersions(
        false,
        sparkVersion,
        Utils.normalizeSparkVersion(sparkVersion),
        supportedSparkMajorMinorVersionPrefix,
        supportedSparkVersions)
    }
  }

  @Test
  def shouldIgnorePatchVersion(): Unit = {
    val sparkVersion = "4.0.5"

    Utils.validateSparkVersions(
      true,
      sparkVersion,
      Utils.normalizeSparkVersion(sparkVersion),
      supportedSparkMajorMinorVersionPrefix,
      supportedSparkVersions)
  }

  @Test
  def shouldThrowForUnsupportedVersion(): Unit = {
    val sparkVersion = "4.0.5"
    val normalizedSparkVersion = Utils.normalizeSparkVersion(sparkVersion)

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Utils.validateSparkVersions(
          false,
          sparkVersion,
          normalizedSparkVersion,
          supportedSparkMajorMinorVersionPrefix,
          supportedSparkVersions)
      })

    assertEquals(
      s"Unsupported spark version used: '$sparkVersion'. " +
        s"Normalized spark version used: '$normalizedSparkVersion'. " +
        s"Supported versions: '${supportedSparkVersions.toSeq.sorted.mkString(", ")}'." +
        "Patch version can be ignored, use setting 'spark.dotnet.ignoreSparkPatchVersionCheck'",

      exception.getMessage)
  }

  @Test
  def shouldThrowForUnsupportedMajorMinorVersion(): Unit = {
    val sparkVersion = "3.5.3"
    val normalizedSparkVersion = Utils.normalizeSparkVersion(sparkVersion)

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Utils.validateSparkVersions(
          false,
          sparkVersion,
          normalizedSparkVersion,
          supportedSparkMajorMinorVersionPrefix,
          supportedSparkVersions)
      })

    assertEquals(
      s"Unsupported spark version used: '$sparkVersion'. " +
        s"Normalized spark version used: '$normalizedSparkVersion'. " +
        s"Supported spark major.minor version: '$supportedSparkMajorMinorVersionPrefix'.",
      exception.getMessage)
  }

  @Test
  def shouldThrowForUnsupportedFutureMajorMinorVersion(): Unit = {
    val sparkVersion = "4.1.0"
    val normalizedSparkVersion = Utils.normalizeSparkVersion(sparkVersion)

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Utils.validateSparkVersions(
          false,
          sparkVersion,
          normalizedSparkVersion,
          supportedSparkMajorMinorVersionPrefix,
          supportedSparkVersions)
      })

    assertEquals(
      s"Unsupported spark version used: '$sparkVersion'. " +
        s"Normalized spark version used: '$normalizedSparkVersion'. " +
        s"Supported spark major.minor version: '$supportedSparkMajorMinorVersionPrefix'.",
      exception.getMessage)
  }

  @Test
  def shouldNotIgnoreUnsupportedMajorMinorVersion(): Unit = {
    val sparkVersion = "4.1.0"
    val normalizedSparkVersion = Utils.normalizeSparkVersion(sparkVersion)

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Utils.validateSparkVersions(
          true,
          sparkVersion,
          normalizedSparkVersion,
          supportedSparkMajorMinorVersionPrefix,
          supportedSparkVersions)
      })

    assertEquals(
      s"Unsupported spark version used: '$sparkVersion'. " +
        s"Normalized spark version used: '$normalizedSparkVersion'. " +
        s"Supported spark major.minor version: '$supportedSparkMajorMinorVersionPrefix'.",
      exception.getMessage)
  }
}
