/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.io.DataOutputStream

import org.apache.spark.internal.Logging

import scala.collection.mutable.Queue

/**
 * CallbackClient is used to communicate with the Dotnet CallbackServer.
 * The client manages and maintains a pool of open CallbackConnections.
 * Any callback request is delegated to a new CallbackConnection or
 * unused CallbackConnection.
 * @param address The address of the Dotnet CallbackServer
 * @param port The port of the Dotnet CallbackServer
 */
class CallbackClient(serDe: SerDe, address: String, port: Int) extends Logging {
  private[this] val connectionPool: Queue[CallbackConnection] = Queue[CallbackConnection]()

  private[this] var isShutdown: Boolean = false

  final def send(callbackId: Int, writeBody: (DataOutputStream, SerDe) => Unit): Unit =
    getOrCreateConnection() match {
      case Some(connection) =>
        try {
          connection.send(callbackId, writeBody)
          addConnection(connection)
        } catch {
          case e: Exception =>
            logError(s"Error calling callback [callback id = $callbackId].", e)
            connection.close()
            throw e
        }
      case None => throw new Exception("Unable to get or create connection.")
    }

  private def getOrCreateConnection(): Option[CallbackConnection] = synchronized {
    if (isShutdown) {
      logInfo("Cannot get or create connection while client is shutdown.")
      return None
    }

    if (connectionPool.nonEmpty) {
      return Some(connectionPool.dequeue())
    }

    Some(new CallbackConnection(serDe, address, port))
  }

  private def addConnection(connection: CallbackConnection): Unit = synchronized {
    assert(connection != null)
    connectionPool.enqueue(connection)
  }

  def shutdown(): Unit = synchronized {
    if (isShutdown) {
      logInfo("Shutdown called, but already shutdown.")
      return
    }

    logInfo("Shutting down.")
    connectionPool.foreach(_.close)
    connectionPool.clear
    isShutdown = true
  }
}
