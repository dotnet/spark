/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.util.dotnet

import org.apache.spark.SparkConf
import org.apache.spark.internal.config.dotnet.Dotnet.DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.function.ThrowingRunnable

@Test
class UtilsTest {

  @Test
  def shouldIgnorePatchVersion(): Unit = {
    val conf = new SparkConf()
    conf.set(DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK, true)

    val sparkVersion = "2.4.8"
    val sparkMajorMinorVersionPrefix = "2.4."
    val supportedSparkVersions =
      Set[String]("2.4.0", "2.4.1", "2.4.3", "2.4.4", "2.4.5", "2.4.6", "2.4.7")

    Utils.validateSparkVersions(
      conf,
      sparkVersion,
      Utils.normalizeSparkVersion(sparkVersion),
      sparkMajorMinorVersionPrefix,
      supportedSparkVersions)
  }

  @Test
  def shouldThrowForUnsupportedVersion(): Unit = {
    val conf = new SparkConf()
    conf.set(DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK, false)

    val sparkVersion = "2.4.8"
    val sparkMajorMinorVersionPrefix = "2.4."
    val supportedSparkVersions =
      Set[String]("2.4.0", "2.4.1", "2.4.3", "2.4.4", "2.4.5", "2.4.6", "2.4.7")

    assertThrows(
      classOf[IllegalArgumentException],
      new ThrowingRunnable {
        override def run(): Unit = {
          Utils.validateSparkVersions(
            conf,
            sparkVersion,
            Utils.normalizeSparkVersion(sparkVersion),
            sparkMajorMinorVersionPrefix,
            supportedSparkVersions)
        }
      })
  }

  @Test
  def shouldThrowForUnsupportedMajorMinorVersion(): Unit = {
    val conf = new SparkConf()
    conf.set(DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK, false)

    val sparkVersion = "3.0.2"
    val sparkMajorMinorVersionPrefix = "2.4."
    val supportedSparkVersions =
      Set[String]("2.4.0", "2.4.1", "2.4.3", "2.4.4", "2.4.5", "2.4.6", "2.4.7")

    assertThrows(
      classOf[IllegalArgumentException],
      new ThrowingRunnable {
        override def run(): Unit = {
          Utils.validateSparkVersions(
            conf,
            sparkVersion,
            Utils.normalizeSparkVersion(sparkVersion),
            sparkMajorMinorVersionPrefix,
            supportedSparkVersions)
        }
      })
  }
}
