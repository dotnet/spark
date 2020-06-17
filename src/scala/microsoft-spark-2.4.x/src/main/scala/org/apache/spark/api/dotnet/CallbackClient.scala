/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.io.{DataInputStream, DataOutputStream}

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
class CallbackClient(address: String, port: Int) extends Logging {
  private[this] val connectionPool = Queue[CallbackConnection]()

  private[this] var isShutdown = false

  final def send[T](writeBody: DataOutputStream => Unit,
                    readBody: Option[DataInputStream => T],
                    retries: Int = 3): Option[T] = {
    val connection = getOrCreateConnection()
    if (connection == null) {
      throw new Exception("Unable to get or create connection.")
    }

    try {
      connection.send(writeBody, readBody) match {
        case CallbackResponse(ConnectionStatus.OK, response) => {
          addConnection(connection)
          response
        }
        case CallbackResponse(ConnectionStatus.ERROR_WRITE, _) => {
          if (retries > 0) {
            logWarning("Error writing to connection, retrying callback.")
            send(writeBody, readBody, retries - 1)
          }

          throw new Exception("Error writing to connection.")
        }
        case CallbackResponse(state, _) =>
          throw new Exception(s"Error encountered with connection: '$state'")
      }
    } catch {
      case e: Exception => {
        logError("Error calling callback.", e)
        connection.close()
        throw e
      }
    }
  }

  private def getOrCreateConnection(): CallbackConnection = synchronized {
    if (isShutdown) {
      logInfo("Cannot get or create connection while client is shutdown.")
      return null
    }

    if (connectionPool.nonEmpty) {
      return connectionPool.dequeue()
    }

    new CallbackConnection(address, port)
  }

  private def addConnection(connection: CallbackConnection): Unit = synchronized {
    if (connection != null) {
      connectionPool.enqueue(connection)
    }
  }

  def shutdown(): Unit = synchronized {
    if (isShutdown) {
      return
    }

    connectionPool.foreach(_.close)
    connectionPool.clear
    isShutdown = true
  }
}
