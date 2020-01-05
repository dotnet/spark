/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.api.dotnet

import java.util.{List => JList, Map => JMap}

import org.apache.spark.api.python.{PythonAccumulatorV2, PythonBroadcast, PythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.dotnet.DotnetRunner


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
    // DOTNET_WORKER_SPARK_VERSION is used to handle different versions of Spark on the worker.
    envVars.put("DOTNET_WORKER_SPARK_VERSION", DotnetRunner.SPARK_VERSION)

    PythonFunction(
      command,
      envVars,
      pythonIncludes,
      pythonExec,
      pythonVersion,
      broadcastVars,
      accumulator)
  }
}
