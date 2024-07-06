/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */


package org.apache.spark.api.dotnet

import Extensions._
import org.junit.Assert._
import org.junit.{After, Before, Test}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

@Test
class DotnetBackendHandlerTest {
  private var backend: DotnetBackend = _
  private var tracker: JVMObjectTracker = _
  private var handler: DotnetBackendHandler = _

  @Before
  def before(): Unit = {
    backend = new DotnetBackend
    tracker = new JVMObjectTracker
    handler = new DotnetBackendHandler(backend, tracker)
  }

  @After
  def after(): Unit = {
    backend.close()
  }

  @Test
  def shouldTrackCallbackClientWhenDotnetProcessConnected(): Unit = {
    val message = givenMessage(m => {
      val serDe = new SerDe(null)
      m.writeBoolean(true) // static method
      serDe.writeInt(m, 1) // processId
      serDe.writeInt(m, 1) // threadId
      serDe.writeString(m, "DotnetHandler") // class name
      serDe.writeString(m, "connectCallback") // command (method) name
      m.writeInt(2) // number of arguments
      m.writeByte('c') // 1st argument type (string)
      serDe.writeString(m, "127.0.0.1") // 1st argument value (host)
      m.writeByte('i') // 2nd argument type (integer)
      m.writeInt(0) // 2nd argument value (port)
    })

    val payload = handler.handleBackendRequest(message)
    val reply = new DataInputStream(new ByteArrayInputStream(payload))

    assertEquals(
      "status code must be successful.", 0, reply.readInt())
    assertEquals('j', reply.readByte())
    assertEquals(1, reply.readInt())
    val trackingId = new String(reply.readNBytes(1), "UTF-8")
    assertEquals("1", trackingId)
    val client = tracker.get(trackingId).get.asInstanceOf[Option[CallbackClient]].orNull
    assertEquals(classOf[CallbackClient], client.getClass)
  }

  private def givenMessage(func: DataOutputStream => Unit): Array[Byte] = {
    val buffer = new ByteArrayOutputStream()
    func(new DataOutputStream(buffer))
    buffer.toByteArray
  }
}
