/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.api.dotnet

import org.apache.spark.api.dotnet.CallbackClient
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter

class DotnetForeachBatchFunction(callbackClient: CallbackClient, callbackId: Int) extends Logging {
  def call(batchDF: DataFrame, batchId: Long): Unit =
    callbackClient.send(
      callbackId,
      (dos, serDe) => {
        serDe.writeJObj(dos, batchDF)
        serDe.writeLong(dos, batchId)
      })
}

object DotnetForeachBatchHelper {
  def callForeachBatch(client: Option[CallbackClient], dsw: DataStreamWriter[Row], callbackId: Int): Unit = {
    val dotnetForeachFunc = client match {
      case Some(value) => new DotnetForeachBatchFunction(value, callbackId)
      case None => throw new Exception("CallbackClient is null.")
    }

    dsw.foreachBatch(dotnetForeachFunc.call _)
  }
}
