/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.test

import java.nio.ByteBuffer
import java.nio.channels.{Pipe, ReadableByteChannel}
import java.util.concurrent.atomic.AtomicInteger

import io.netty.buffer.Unpooled
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.Assert._
import org.junit.Test

import org.apache.spark.network.client.{StreamCallback, TransportClient, TransportResponseHandler}
import org.apache.spark.network.protocol.{
  ResponseMessage, StreamFailure, StreamRequest, StreamResponse}
import org.apache.spark.network.util.TransportFrameDecoder
import org.apache.spark.sql.test.E2ETransportDiagnostics.{ClientState, Recorder}

class E2ETransportDiagnosticsTest {
  private class Source(val channel: ReadableByteChannel) extends ReadableByteChannel {
    @volatile var error: Throwable = _
    override def read(buffer: ByteBuffer): Int = channel.read(buffer)
    override def isOpen: Boolean = channel.isOpen
    override def close(): Unit = channel.close()
  }

  private class Download extends StreamCallback {
    private val pipe = Pipe.open()
    val sink = pipe.sink()
    val source = new Source(pipe.source())
    val failures = new AtomicInteger()
    override def onData(id: String, buffer: ByteBuffer): Unit = { sink.write(buffer); () }
    override def onComplete(id: String): Unit = sink.close()
    override def onFailure(id: String, cause: Throwable): Unit = {
      failures.incrementAndGet()
      source.error = cause
      sink.close()
    }
    def close(): Unit = { sink.close(); source.close() }
  }

  private def configuredChannel(recorder: Recorder): (EmbeddedChannel,
      TransportResponseHandler, ClientState) = {
    val channel = new EmbeddedChannel()
    val responses = new TransportResponseHandler(channel)
    val state = new ClientState(new TransportClient(channel, responses), 1L, recorder)
    channel.pipeline().addLast("probe", state.probe)
    channel.pipeline().addLast("handler", new ChannelInboundHandlerAdapter {
      override def channelRead(ctx: ChannelHandlerContext, message: Any): Unit = message match {
        case response: ResponseMessage => responses.handle(response)
        case _ => ctx.fireChannelRead(message)
      }
    })
    (channel, responses, state)
  }

  @Test
  def forwardsRequestAndRecordsMissingClassCompletionWithoutSecrets(): Unit = {
    val recorder = new Recorder()
    val (channel, responses, _) = configuredChannel(recorder)
    val download = new Download()
    val secret = "spark://private-host:1234/customer-secret.class?token=secret"
    try {
      responses.addStreamCallback(secret, download)
      val request = new StreamRequest(secret)
      assertTrue(channel.writeOutbound(request))
      assertSame(request, channel.readOutbound[AnyRef]())
      assertFalse(channel.writeInbound(
        new StreamFailure(secret, s"Stream '$secret' was not found.")))
      assertEquals(1, download.failures.get())
      assertFalse(download.sink.isOpen)
      assertEquals(0, responses.numOutstandingRequests())
      val lines = recorder.drain()
      assertTrue(lines.exists(line => line.contains("event=response_before ") &&
        line.contains("missing=1") && line.contains("matched=1")))
      assertTrue(lines.exists(line => line.contains("event=response_after ") &&
        line.contains("sink_open=0") && line.contains("source_error=1")))
      assertTrue(lines.exists(_.contains("event=write_result ")))
      assertTrue(lines.exists(_.contains("last_request_age_ms=")))
      assertTrue(lines.forall(!_.contains("secret")))
      assertTrue(lines.forall(!_.contains("private-host")))
      assertTrue(lines.forall(_.matches(
        "SPARK_TRANSPORT_DIAG event=[a-z_]+(?: [a-z_]+=-?[0-9]+)*")))
    } finally { download.close(); channel.finishAndReleaseAll() }
  }

  @Test
  def forwardsUnrelatedBufferExactlyOnceWithoutChangingItsOwnership(): Unit = {
    val recorder = new Recorder()
    val (channel, _, _) = configuredChannel(recorder)
    val buffer = Unpooled.wrappedBuffer(Array[Byte](1, 2, 3))
    try {
      assertTrue(channel.writeInbound(buffer))
      assertSame(buffer, channel.readInbound[AnyRef]())
      assertEquals(0, buffer.readerIndex())
      assertEquals(1, buffer.refCnt())
      assertNull(channel.readInbound[AnyRef]())
      assertTrue(recorder.drain().isEmpty)
    } finally { buffer.release(); channel.finishAndReleaseAll() }
  }

  @Test
  def reflectionFailureDoesNotPreventOrDuplicateTheRealCallback(): Unit = {
    val recorder = new Recorder()
    val (channel, responses, _) = configuredChannel(recorder)
    val calls = new AtomicInteger()
    val callback = new StreamCallback {
      override def onData(id: String, buffer: ByteBuffer): Unit = ()
      override def onComplete(id: String): Unit = ()
      override def onFailure(id: String, cause: Throwable): Unit = { calls.incrementAndGet(); () }
    }
    try {
      responses.addStreamCallback("not-logged", callback)
      channel.writeInbound(new StreamFailure("not-logged", "sensitive failure message"))
      assertEquals(1, calls.get())
      val lines = recorder.drain()
      assertTrue(lines.exists(_.contains("error_type=NoSuchFieldException")))
      assertTrue(lines.forall(!_.contains("sensitive")))
      assertTrue(lines.forall(!_.contains("not-logged")))
    } finally { channel.finishAndReleaseAll() }
  }

  @Test
  def retainsPreHookCallbackAfterItDisappearsFromTheOutstandingQueue(): Unit = {
    val recorder = new Recorder()
    val channel = new EmbeddedChannel()
    val responses = new TransportResponseHandler(channel)
    val state = new ClientState(new TransportClient(channel, responses), 1, recorder)
    val download = new Download()
    try {
      responses.addStreamCallback("missing", download)
      state.seedPending()
      state.snapshot("snapshot", false)
      responses.handle(new StreamFailure("missing", "Stream 'missing' was not found."))
      state.snapshot("snapshot", false)
      assertEquals(0, responses.numOutstandingRequests())
      val lines = recorder.drain()
      assertTrue(lines.exists(_.contains("sink_open=1")))
      assertTrue(lines.exists(_.contains("sink_open=0")))
      assertTrue(lines.forall(_.contains("on_loop=0")))
    } finally { download.close(); channel.finishAndReleaseAll() }
  }

  @Test
  def recoversActiveCallbackWhenResponsePrecedesLazyAttachment(): Unit = {
    val recorder = new Recorder()
    val channel = new EmbeddedChannel()
    channel.pipeline().addLast(TransportFrameDecoder.HANDLER_NAME, new TransportFrameDecoder())
    val responses = new TransportResponseHandler(channel)
    val download = new Download()
    try {
      responses.addStreamCallback("not-logged", download)
      responses.handle(new StreamResponse("not-logged", 3L, null))
      val state = new ClientState(new TransportClient(channel, responses), 1L, recorder)
      state.seedPending()
      assertSame(download, state.latest.get().callback)
      state.snapshot("snapshot", false)
      val lines = recorder.drain()
      assertTrue(lines.exists(line => line.contains("queued=0") &&
        line.contains("interceptor=1")))
      assertTrue(lines.exists(_.contains("sink_open=1")))
      assertFalse(channel.writeInbound(Unpooled.wrappedBuffer(Array[Byte](1, 2, 3))))
      assertFalse(download.sink.isOpen)
      assertEquals(0, download.failures.get())
      val received = ByteBuffer.allocate(3)
      assertEquals(3, download.source.read(received))
      assertArrayEquals(Array[Byte](1, 2, 3), received.array())
    } finally { download.close(); channel.finishAndReleaseAll() }
  }

  @Test
  def recordsUnknownOutboundIdentityWhenSameIdCallbacksAreAmbiguous(): Unit = {
    val recorder = new Recorder()
    val (channel, responses, state) = configuredChannel(recorder)
    val first = new Download()
    val second = new Download()
    try {
      responses.addStreamCallback("same-id", first)
      responses.addStreamCallback("same-id", second)
      state.remember(second) // An old latest value must not leak into an ambiguous request.
      (1 to 2).foreach { _ =>
        val request = new StreamRequest("same-id")
        assertTrue(channel.writeOutbound(request))
        assertSame(request, channel.readOutbound[AnyRef]())
      }
      val lines = recorder.drain()
      assertEquals(2, lines.count(_.contains("event=request ")))
      assertEquals(2, lines.count(_.contains("event=write_result ")))
      assertTrue(lines.forall(line => line.contains("request=-1 ") &&
        line.contains("callback=-1")))
    } finally { first.close(); second.close(); channel.finishAndReleaseAll() }
  }

  @Test
  def boundsQueueAndAllowsOnlyKnownExceptionTypes(): Unit = {
    val recorder = new Recorder(2)
    recorder.event("start")
    recorder.event("snapshot", "channel" -> 1L)
    recorder.event("stop")
    assertEquals(2, recorder.drain().size)
    assertEquals(1L, recorder.dropped.get())
    recorder.error(new RuntimeException("credentials-must-not-appear"))
    recorder.error(new Exception("private-message"))
    assertEquals(Set(
      "SPARK_TRANSPORT_DIAG event=diagnostic_error error_type=RuntimeException",
      "SPARK_TRANSPORT_DIAG event=diagnostic_error error_type=Other"), recorder.drain().toSet)
  }

  @Test
  def retainsOlderUnfinishedCallbackWhenNewerRequestCompletes(): Unit = {
    val recorder = new Recorder()
    val (channel, _, state) = configuredChannel(recorder)
    val orphan = new Download()
    val completed = new Download()
    try {
      val first = state.remember(orphan)
      state.remember(completed)
      completed.onComplete("not-logged")
      state.forgetClosed()
      assertEquals(Seq(first), state.pendingValues)
      state.snapshot("snapshot", false)
      assertTrue(recorder.drain().exists(line =>
        line.contains(s"request=${first.id} ") && line.contains("sink_open=1")))
      recorder.enabled.set(false)
      recorder.safely { throw new AssertionError("disabled diagnostics must not execute") }
      recorder.event("stop")
      assertTrue(recorder.drain().isEmpty)
    } finally { orphan.close(); completed.close(); channel.finishAndReleaseAll() }
  }
}
