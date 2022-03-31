/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.io.{ByteArrayOutputStream, Closeable, DataInputStream, DataOutputStream}
import java.net.Socket

import org.apache.spark.internal.Logging

/**
 * CallbackConnection is used to process the callback communication
 * between the JVM and Dotnet. It uses a TCP socket to communicate with
 * the Dotnet CallbackServer and the socket is expected to be reused.
 * @param address The address of the Dotnet CallbackServer
 * @param port The port of the Dotnet CallbackServer
 */
class CallbackConnection(serDe: SerDe, address: String, port: Int) extends Logging {
  private[this] val socket: Socket = new Socket(address, port)
  private[this] val inputStream: DataInputStream = new DataInputStream(socket.getInputStream)
  private[this] val outputStream: DataOutputStream = new DataOutputStream(socket.getOutputStream)

  def send(
      callbackId: Int,
      writeBody: (DataOutputStream, SerDe) => Unit): Unit = {
    logInfo(s"Calling callback [callback id = $callbackId] ...")

    try {
      serDe.writeInt(outputStream, CallbackFlags.CALLBACK)
      serDe.writeInt(outputStream, callbackId)

      val byteArrayOutputStream = new ByteArrayOutputStream()
      writeBody(new DataOutputStream(byteArrayOutputStream), serDe)
      serDe.writeInt(outputStream, byteArrayOutputStream.size)
      byteArrayOutputStream.writeTo(outputStream);
    } catch {
      case e: Exception => {
        throw new Exception("Error writing to stream.", e)
      }
    }

    logInfo(s"Signaling END_OF_STREAM.")
    try {
      serDe.writeInt(outputStream, CallbackFlags.END_OF_STREAM)
      outputStream.flush()

      val endOfStreamResponse = readFlag(inputStream)
      endOfStreamResponse match {
        case CallbackFlags.END_OF_STREAM =>
          logInfo(s"Received END_OF_STREAM signal. Calling callback [callback id = $callbackId] successful.")
        case _ =>  {
          throw new Exception(s"Error verifying end of stream. Expected: ${CallbackFlags.END_OF_STREAM}, " +
            s"Received: $endOfStreamResponse")
        }
      }
    } catch {
      case e: Exception => {
        throw new Exception("Error while verifying end of stream.", e)
      }
    }
  }

  def close(): Unit = {
    try {
      serDe.writeInt(outputStream, CallbackFlags.CLOSE)
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
      assert(s != null)
      s.close()
    } catch {
      case e: Exception => logInfo("Unable to close socket.", e)
    }
  }

  private def close(c: Closeable): Unit = {
    try {
      assert(c != null)
      c.close()
    } catch {
      case e: Exception => logInfo("Unable to close closeable.", e)
    }
  }

  private def readFlag(inputStream: DataInputStream): Int = {
    val callbackFlag = serDe.readInt(inputStream)
    if (callbackFlag == CallbackFlags.DOTNET_EXCEPTION_THROWN) {
      val exceptionMessage = serDe.readString(inputStream)
      throw new DotnetException(exceptionMessage)
    }
    callbackFlag
  }

  private object CallbackFlags {
    val CLOSE: Int = -1
    val CALLBACK: Int = -2
    val DOTNET_EXCEPTION_THROWN: Int = -3
    val END_OF_STREAM: Int = -4
  }
}
