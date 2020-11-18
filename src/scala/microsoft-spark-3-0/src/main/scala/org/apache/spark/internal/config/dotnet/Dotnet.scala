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
}
