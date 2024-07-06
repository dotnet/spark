/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.api.dotnet

import java.util.{List => JList, Map => JMap}

import org.apache.spark.api.python.{PythonAccumulatorV2, PythonBroadcast, PythonFunction, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast

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
}
