/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.io.{Closeable, DataInputStream, DataOutputStream}
import java.net.Socket

import org.apache.spark.internal.Logging

/**
 * CallbackConnection is used to process the callback communication
 * between the JVM and Dotnet. It uses a TCP socket to communicate with
 * the Dotnet CallbackServer and the socket is expected to be reused.
 * @param address The address of the Dotnet CallbackServer
 * @param port The port of the Dotnet CallbackServer
 */
class CallbackConnection(address: String, port: Int) extends Logging {
  private[this] val socket = new Socket(address, port)
  private[this] val inputStream = new DataInputStream(socket.getInputStream)
  private[this] val outputStream = new DataOutputStream(socket.getOutputStream)

  def send[T](
    writeBody: DataOutputStream => Unit,
    readBody: Option[DataInputStream => T]): CallbackResponse[T] = {
    logInfo("Calling callback...")

    try {
      SerDe.writeInt(outputStream, CallbackFlags.CALLBACK)
      writeBody(outputStream)
      outputStream.flush()
    } catch {
      case e: Exception => {
        logError("Error writing to stream.", e)
        return CallbackResponse(ConnectionStatus.ERROR_WRITE, None)
      }
    }

    val readBodyResponse: Option[T] =
      try {
        readBody match {
          case Some(body) => {
            logInfo("Checking readBody return value flag")
            val returnValueFlag = checkForDotnetException(inputStream)
            if (returnValueFlag != CallbackFlags.CALLBACK_RETURN_VALUE) {
              throw new Exception("readBody defined, however flag to indicate return value not " +
                s"received. Expected: ${CallbackFlags.CALLBACK_RETURN_VALUE}, " +
                s"Received: $returnValueFlag")
            }
            Some(body(inputStream))
          }
          case None => None
        }
      } catch {
        case e: Exception => {
          logError("Error reading stream while checking callback return value.", e)
          return CallbackResponse(ConnectionStatus.ERROR_READ, None)
        }
      }

    logInfo(s"Signaling END_OF_STREAM.")
    try {
      SerDe.writeInt(outputStream, CallbackFlags.END_OF_STREAM)
      outputStream.flush()

      val endOfStreamResponse = checkForDotnetException(inputStream)
      endOfStreamResponse match {
        case CallbackFlags.END_OF_STREAM =>
          logInfo(s"Received END_OF_STREAM signal. Calling callback successful.")
          return CallbackResponse(ConnectionStatus.OK, readBodyResponse)
        case _ =>  {
          logError(s"Error verifying end of stream. Expected: ${CallbackFlags.END_OF_STREAM}, " +
              s"Received: $endOfStreamResponse")
        }
      }
    } catch {
      case e: Exception => {
        logError("Error while verifying end of stream.", e)
      }
    }

    CallbackResponse(ConnectionStatus.ERROR_END_OF_STREAM, readBodyResponse)
  }

  def close(): Unit = {
    try {
      SerDe.writeInt(outputStream, CallbackFlags.CLOSE)
      outputStream.flush()
    } catch {
      case e: Exception => logInfo("Unable to send close to .NET callback server.", e)
    }

    close(socket)
    close(outputStream)
    close(inputStream)
  }

  private def close(s: Socket): Unit = {
    try {
      if (s != null) {
        s.close()
      }
    } catch {
      case e: Exception => logInfo("Unable to close socket.", e)
    }
  }

  private def close(c: Closeable): Unit = {
    try {
      if (c != null) {
        c.close()
      }
    } catch {
      case e: Exception => logInfo("Unable to close closeable.", e)
    }
  }

  private def checkForDotnetException(inputStream: DataInputStream): Int = {
    val callbackFlag = SerDe.readInt(inputStream)
    if (callbackFlag == CallbackFlags.DOTNET_EXCEPTION_THROWN) {
      val exceptionMessage = SerDe.readString(inputStream)
      throw new DotnetException(exceptionMessage)
    }
    callbackFlag
  }

  private object CallbackFlags {
    val CLOSE = -1
    val CALLBACK = -2
    val CALLBACK_RETURN_VALUE = -3
    val DOTNET_EXCEPTION_THROWN = -4
    val END_OF_STREAM = -5
  }
}

object ConnectionStatus extends Enumeration {
  type ConnectionStatus = Value
  val OK, ERROR_WRITE, ERROR_READ, ERROR_END_OF_STREAM = Value
}

case class CallbackResponse[T](state: ConnectionStatus.ConnectionStatus, response: Option[T]);
