/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.api.dotnet

import org.apache.spark.api.dotnet.{DotnetBackend, SerDe}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

class DotnetForeachBatchFunction(callbackId: Int) extends Logging{
  def call(batchDF: DataFrame, batchId: Long): Unit = {
    DotnetBackend.callbackClient.send(
      dos => {
        SerDe.writeInt(dos, callbackId)
        SerDe.writeJObj(dos, batchDF)
        SerDe.writeLong(dos, batchId)
      }, None)
  }
}

object DotnetForeachBatchHelper {
  def callForeachBatch(dsw: DataStreamWriter[Row], callbackId: Int): Unit = {
    val dotnetForeachFunc = new DotnetForeachBatchFunction(callbackId)
    dsw.foreachBatch(dotnetForeachFunc.call _)
  }
}
