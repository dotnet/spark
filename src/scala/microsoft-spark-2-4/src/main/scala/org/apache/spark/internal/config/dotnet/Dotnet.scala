/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.internal.config.dotnet

import org.apache.spark.internal.config.ConfigBuilder

private[spark] object Dotnet {
  val DOTNET_NUM_BACKEND_THREADS = ConfigBuilder("spark.dotnet.numDotnetBackendThreads").intConf
    .createWithDefault(10)

  val DOTNET_IGNORE_SPARK_PATCH_VERSION_CHECK =
    ConfigBuilder("spark.dotnet.ignoreSparkPatchVersionCheck").booleanConf
      .createWithDefault(false)

  val ERROR_REDIRECITON_ENABLED =
    ConfigBuilder("spark.nonjvm.error.forwarding.enabled").booleanConf
      .createWithDefault(false)

  val ERROR_BUFFER_SIZE =
    ConfigBuilder("spark.nonjvm.error.buffer.size")
      .intConf
      .checkValue(_ >= 0, "The error buffer size must not be negative")
      .createWithDefault(10240)
}
