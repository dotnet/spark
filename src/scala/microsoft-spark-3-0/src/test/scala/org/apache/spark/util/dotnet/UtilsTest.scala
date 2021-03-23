/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.util.dotnet

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.dotnet.Dotnet.DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK
import org.junit.Assert.{assertEquals, assertThrows}
import org.junit.Test

@Test
class UtilsTest {

  @Test
  def shouldIgnorePatchVersion(): Unit = {
    val sparkVersion = "3.0.3"
    val sparkMajorMinorVersionPrefix = "3.0"
    val supportedSparkVersions = Set[String]("3.0.0", "3.0.1", "3.0.2")

    Utils.validateSparkVersions(
      true,
      sparkVersion,
      Utils.normalizeSparkVersion(sparkVersion),
      sparkMajorMinorVersionPrefix,
      supportedSparkVersions)
  }

  @Test
  def shouldThrowForUnsupportedVersion(): Unit = {
    val sparkVersion = "3.0.3"
    val normalizedSparkVersion = Utils.normalizeSparkVersion(sparkVersion)
    val sparkMajorMinorVersionPrefix = "3.0"
    val supportedSparkVersions = Set[String]("3.0.0", "3.0.1", "3.0.2")

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Utils.validateSparkVersions(
          false,
          sparkVersion,
          normalizedSparkVersion,
          sparkMajorMinorVersionPrefix,
          supportedSparkVersions)
      })

    assertEquals(
      s"Unsupported spark version used: '$sparkVersion'. " +
        s"Normalized spark version used: '$normalizedSparkVersion'. " +
        s"Supported versions: '${supportedSparkVersions.toSeq.sorted.mkString(", ")}'.",
      exception.getMessage)
  }

  @Test
  def shouldThrowForUnsupportedMajorMinorVersion(): Unit = {
    val sparkVersion = "2.4.4"
    val normalizedSparkVersion = Utils.normalizeSparkVersion(sparkVersion)
    val sparkMajorMinorVersionPrefix = "3.0"
    val supportedSparkVersions = Set[String]("3.0.0", "3.0.1", "3.0.2")

    val exception = assertThrows(
      classOf[IllegalArgumentException],
      () => {
        Utils.validateSparkVersions(
          false,
          sparkVersion,
          normalizedSparkVersion,
          sparkMajorMinorVersionPrefix,
          supportedSparkVersions)
      })

    assertEquals(
      s"Unsupported spark version used: '$sparkVersion'. " +
        s"Normalized spark version used: '$normalizedSparkVersion'. " +
        s"Supported spark major.minor version: '$sparkMajorMinorVersionPrefix'.",
      exception.getMessage)
  }
}
