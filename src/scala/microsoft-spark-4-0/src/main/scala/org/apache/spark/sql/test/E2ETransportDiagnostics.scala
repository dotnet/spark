/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.test

import java.io.IOException
import java.nio.channels.Channel
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.netty.channel.{ChannelDuplexHandler, ChannelHandlerContext, ChannelPromise}
import org.apache.commons.lang3.tuple.Pair

import org.apache.spark.SparkEnv
import org.apache.spark.network.client.{StreamCallback, TransportClient}
import org.apache.spark.network.protocol.{StreamFailure, StreamRequest, StreamResponse}
import org.apache.spark.network.util.{TransportConf, TransportFrameDecoder}

/** Debug-branch only: observes downloads, never retries or completes their callbacks. */
object E2ETransportDiagnostics {
  private val probeName = "dotnetE2ETransportDiagnostics"
  @volatile private var current: Observer = _

  def start(): Boolean = synchronized {
    if (current != null) return !current.stopped.get()
    if (System.getenv("DOTNET_SPARK_DEBUG_TRANSPORT") != "1") return false
    val env = SparkEnv.get
    if (env == null || env.isStopped) return false
    try {
      val observer = new Observer(env)
      observer.setDaemon(true)
      observer.setContextClassLoader(getClass.getClassLoader)
      observer.start()
      current = observer
      true // The observer started; individual channels may not have been discovered yet.
    } catch { case NonFatal(_) => false }
  }

  def stop(): Unit = synchronized {
    if (current != null) current.stopped.set(true)
  }

  private[test] def field(target: AnyRef, name: String): AnyRef = {
    var owner: Class[_] = target.getClass
    while (owner != null) {
      try {
        val member = owner.getDeclaredField(name)
        member.setAccessible(true)
        return member.get(target)
      } catch { case _: NoSuchFieldException => owner = owner.getSuperclass }
    }
    throw new NoSuchFieldException(name)
  }

  private def flag(value: Boolean): Long = if (value) 1L else 0L

  private[test] final class Recorder(val capacity: Int = 2048) {
    private val queue = new ConcurrentLinkedQueue[String]()
    private val size = new AtomicInteger()
    private val sequence = new AtomicLong()
    private val errors = new AtomicInteger()
    private val started = System.nanoTime()
    val enabled = new AtomicBoolean(true)
    val dropped = new AtomicLong()

    def event(name: String, values: (String, Long)*): Unit = {
      if (!enabled.get()) return
      offer("SPARK_TRANSPORT_DIAG event=" + name + " seq=" + sequence.incrementAndGet() +
        " elapsed_ms=" + (System.nanoTime() - started) / 1000000L +
        values.map { case (key, value) => s" $key=$value" }.mkString)
    }

    def error(cause: Throwable): Unit = {
      if (errors.incrementAndGet() <= 16) {
        val kind = cause match {
          case _: NoSuchFieldException => "NoSuchFieldException"
          case _: IllegalAccessException => "IllegalAccessException"
          case _: SecurityException => "SecurityException"
          case _: IllegalArgumentException => "IllegalArgumentException"
          case _: IOException => "IOException"
          case _: RuntimeException => "RuntimeException"
          case _ => "Other"
        }
        offer(s"SPARK_TRANSPORT_DIAG event=diagnostic_error error_type=$kind")
      }
    }

    def safely(action: => Unit): Unit = {
      if (enabled.get()) try action catch { case NonFatal(e) => error(e) }
    }

    private def offer(line: String): Unit = {
      if (size.incrementAndGet() <= capacity) queue.offer(line)
      else { size.decrementAndGet(); dropped.incrementAndGet() }
    }

    def drain(): Seq[String] = {
      val result = Vector.newBuilder[String]
      var remaining = capacity
      while (remaining > 0) {
        val line = queue.poll()
        if (line == null) remaining = 0
        else { size.decrementAndGet(); result += line; remaining -= 1 }
      }
      result.result()
    }
  }

  private[test] final class Pending(val callback: AnyRef, val id: Long) {
    val started = System.nanoTime()
    def age: Long = (System.nanoTime() - started) / 1000000L
  }

  private[test] final class ClientState(
      val client: TransportClient,
      val id: Long,
      val recorder: Recorder,
      val timeout: Long = -1L) {
    val latest = new AtomicReference[Pending]()
    val attachRequested = new AtomicBoolean()
    val snapshotRequested = new AtomicBoolean()
    private val requestIds = new AtomicLong()
    private val retained = new ConcurrentLinkedQueue[Pending]()
    private val retainedCount = new AtomicInteger()
    var flushedRequest = -1L // Observer thread only.

    private def callbacks = field(client.getHandler, "streamCallbacks")
      .asInstanceOf[java.util.Queue[Pair[String, StreamCallback]]]

    def remember(callback: AnyRef): Pending = {
      val previous = latest.get()
      if (previous != null && (previous.callback eq callback)) previous
      else {
        val pending = retained.iterator().asScala.find(_.callback eq callback).getOrElse {
          val value = new Pending(callback, requestIds.incrementAndGet())
          retain(value)
          value
        }
        latest.set(pending)
        pending
      }
    }

    private def retain(pending: Pending): Unit = {
      if (retainedCount.incrementAndGet() <= 64) retained.offer(pending)
      else { retainedCount.decrementAndGet(); recorder.dropped.incrementAndGet() }
    }

    def pendingValues: Seq[Pending] = retained.iterator().asScala.toVector

    def forgetClosed(): Unit = pendingValues.foreach { pending => recorder.safely {
      if (!field(pending.callback, "sink").asInstanceOf[Channel].isOpen &&
          retained.remove(pending)) retainedCount.decrementAndGet()
    }}

    def seedPending(): Unit = {
      if (latest.get() != null) return
      val decoder = client.getChannel.pipeline().get(TransportFrameDecoder.HANDLER_NAME)
      val interceptor = if (decoder == null) null else field(decoder, "interceptor")
      val head = callbacks.peek()
      // A response received before attachment has already moved its callback out of the queue.
      val callback = if (interceptor != null) field(interceptor, "callback")
        else if (head != null) head.getValue else null
      if (callback != null) {
        val pending = new Pending(callback, requestIds.incrementAndGet())
        if (latest.compareAndSet(null, pending)) retain(pending)
      }
    }

    def snapshot(event: String, onLoop: Boolean, extra: (String, Long)*): Unit = {
      val channel = client.getChannel
      val loop = channel.eventLoop()
      val decoder = channel.pipeline().get(TransportFrameDecoder.HANDLER_NAME)
      val interceptor = if (decoder == null) null else field(decoder, "interceptor")
      val pending = latest.get()
      val lastRequest = client.getHandler.getTimeOfLastRequestNs()
      val values = Vector("channel" -> id, "on_loop" -> flag(onLoop),
        "loop" -> System.identityHashCode(loop).toLong,
        "thread" -> Thread.currentThread().getId, "open" -> flag(channel.isOpen),
        "connected" -> flag(channel.isActive), "writable" -> flag(channel.isWritable),
        "loop_shutdown" -> flag(loop.isShuttingDown),
        "loop_terminated" -> flag(loop.isTerminated), "timeout_ms" -> timeout,
        "last_request_age_ms" -> (if (lastRequest == 0L) -1L
          else (System.nanoTime() - lastRequest) / 1000000L),
        "queued" -> callbacks.size().toLong,
        "active" -> flag(field(client.getHandler, "streamActive").asInstanceOf[Boolean]),
        "interceptor" -> flag(interceptor != null),
        "request" -> (if (pending == null) -1L else pending.id),
        "callback" -> (if (pending == null) -1L
          else System.identityHashCode(pending.callback).toLong),
        "age_ms" -> (if (pending == null) -1L else pending.age))
      // Emit liveness even when private callback fields differ in another Spark patch release.
      recorder.event(event, (values ++ extra): _*)
      val observed = if (event == "snapshot") (pendingValues ++ Option(pending)).distinct
        else Option(pending).toSeq
      observed.foreach { value => recorder.safely {
        val source = field(value.callback, "source")
        val sink = field(value.callback, "sink").asInstanceOf[Channel]
        recorder.event(event, "channel" -> id, "on_loop" -> flag(onLoop),
          "request" -> value.id, "callback" -> System.identityHashCode(value.callback).toLong,
          "source_open" -> flag(source.asInstanceOf[Channel].isOpen),
          "sink_open" -> flag(sink.isOpen), "source_error" -> flag(field(source, "error") != null))
      }}
      if (interceptor != null) recorder.safely {
        recorder.event(event, "channel" -> id, "on_loop" -> flag(onLoop),
          "bytes_read" -> field(interceptor, "bytesRead").asInstanceOf[Long],
          "byte_count" -> field(interceptor, "byteCount").asInstanceOf[Long])
      }
    }

    private[test] def probe: ChannelDuplexHandler = new ChannelDuplexHandler {
      override def write(
          ctx: ChannelHandlerContext, message: Any, promise: ChannelPromise): Unit = {
        recorder.safely {
          message match {
            case request: StreamRequest =>
              val matches = callbacks.iterator().asScala
                .filter(_.getKey == request.streamId).take(2).toVector
              // Same-ID callbacks may be queued before either write; do not guess their order.
              val pending = if (matches.size == 1) Some(remember(matches.head.getValue)) else None
              if (pending.isDefined) snapshot("request", true)
              else recorder.event("request", "channel" -> id, "request" -> -1L,
                "callback" -> -1L)
              promise.addListener((future: io.netty.util.concurrent.Future[_ >: Void]) =>
                recorder.safely {
                  recorder.event("write_result", "channel" -> id, "ok" -> flag(future.isSuccess),
                    "request" -> pending.map(_.id).getOrElse(-1L),
                    "callback" -> pending.map(p =>
                      System.identityHashCode(p.callback).toLong).getOrElse(-1L))
                })
            case _ =>
          }
        }
        ctx.write(message, promise) // Forward exactly once, even if diagnostics failed.
      }

      override def channelRead(ctx: ChannelHandlerContext, message: Any): Unit = {
        val response = message.isInstanceOf[StreamResponse] || message.isInstanceOf[StreamFailure]
        if (response) recorder.safely {
          val head = callbacks.peek()
          if (head != null) remember(head.getValue)
          val (streamId, count, missing) = message match {
            case value: StreamResponse => (value.streamId, value.byteCount, 0L)
            case value: StreamFailure =>
              (value.streamId, -1L, flag(value.error.startsWith("Stream '") &&
                value.error.endsWith("' was not found.")))
          }
          snapshot("response_before", true, "byte_count" -> count, "missing" -> missing,
            "matched" -> flag(head != null && head.getKey == streamId))
        }
        ctx.fireChannelRead(message)
        if (response) recorder.safely { snapshot("response_after", true); forgetClosed() }
      }

      override def channelInactive(ctx: ChannelHandlerContext): Unit = {
        recorder.safely { snapshot("channel_inactive", true) }
        ctx.fireChannelInactive()
      }

      override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
        recorder.safely {
          snapshot("channel_exception", true)
          recorder.error(cause)
        }
        ctx.fireExceptionCaught(cause)
      }
    }
  }

  private final class Observer(env: SparkEnv) extends Thread("dotnet-e2e-transport-diagnostics") {
    val stopped = new AtomicBoolean()
    private val recorder = new Recorder()
    private val clients = new ConcurrentHashMap[TransportClient, ClientState]()
    private val recent = new java.util.ArrayDeque[String]()
    private var nextChannel = 0L
    private var outputBytes = 0L
    private var outputLimited = false
    private var normalEventsWritten = 0

    private def write(line: String): Unit = {
      if (outputBytes + line.length + 1 <= 1024 * 1024 - 128 && !outputLimited) {
        System.err.println(line) // Never called by a Netty event loop or a download callback.
        outputBytes += line.length + 1
      } else if (!outputLimited) {
        System.err.println("SPARK_TRANSPORT_DIAG event=limit bytes_read=" + outputBytes)
        outputLimited = true
        stopped.set(true)
      }
    }

    private def drain(): Unit = recorder.drain().foreach { line =>
      if (line.contains("event=request ") || line.contains("event=write_result ") ||
          line.contains("event=response_")) {
        // Publish a bounded sample before JVM shutdown can interrupt the daemon's final flush.
        if (normalEventsWritten < 32) {
          write(line)
          normalEventsWritten += 1
        }
        if (recent.size() == 128) recent.removeFirst()
        recent.addLast(line)
      } else write(line)
    }

    private def discover(): Unit = {
      // Read the field, not downloadClient(): do not initialize a factory or open a connection.
      val factory = field(env.rpcEnv, "fileDownloadFactory")
      if (factory != null) {
        val pools = field(factory, "connectionPool").asInstanceOf[java.util.Map[_, _]]
        val timeout = field(factory, "conf").asInstanceOf[TransportConf].connectionTimeoutMs()
        pools.values().asScala.take(16).foreach { pool =>
          field(pool.asInstanceOf[AnyRef], "clients").asInstanceOf[Array[TransportClient]]
            .filter(_ != null).take(16).foreach { client =>
              if (!clients.containsKey(client) && clients.size() < 16) {
                nextChannel += 1
                clients.put(client, new ClientState(client, nextChannel, recorder, timeout))
              }
            }
        }
      }
      clients.values().asScala.foreach { state => recorder.safely {
        // Preserve an already-pending callback before the lazy hook is installed.
        state.seedPending()
        val channel = state.client.getChannel
        if (channel.isOpen && state.attachRequested.compareAndSet(false, true)) {
          recorder.event("attach_requested", "channel" -> state.id)
          channel.eventLoop().execute(new Runnable {
            override def run(): Unit = recorder.safely {
              if (!stopped.get() && channel.pipeline().get(probeName) == null) {
                channel.pipeline().addBefore("handler", probeName, state.probe)
                recorder.event("attached", "channel" -> state.id)
              }
            }
          })
        }
      }}
    }

    override def run(): Unit = {
      val started = System.nanoTime()
      var nextSnapshot = started
      recorder.event("start")
      try {
        while (!stopped.get() && !env.isStopped && (SparkEnv.get eq env) &&
            System.nanoTime() - started < 15L * 60 * 1000000000L) {
          recorder.safely { discover() }
          if (System.nanoTime() >= nextSnapshot) {
            clients.values().asScala.foreach { state => recorder.safely {
              // Off-loop reflection is a weak snapshot; it does not acquire Spark/Netty locks.
              state.snapshot("snapshot", false)
              state.forgetClosed()
              val channel = state.client.getChannel
              if (channel.isOpen && !channel.eventLoop().isShuttingDown &&
                  state.snapshotRequested.compareAndSet(false, true)) {
                channel.eventLoop().execute(new Runnable {
                  override def run(): Unit = try {
                    recorder.safely { state.snapshot("snapshot", true) }
                  } finally { state.snapshotRequested.set(false) }
                })
              }
              state.pendingValues.find(_.age >= 10000).foreach { pending =>
                if (state.flushedRequest != pending.id) {
                  drain()
                  recent.iterator().asScala.foreach(write)
                  recent.clear()
                  state.flushedRequest = pending.id
                }
              }
            }}
            recorder.event("snapshot", "dropped" -> recorder.dropped.get())
            nextSnapshot = System.nanoTime() + 5L * 1000000000L
          }
          drain()
          Thread.sleep(250)
        }
      } catch { case NonFatal(e) => recorder.error(e) }
      finally {
        recorder.event("stop")
        drain()
        recent.iterator().asScala.foreach(write)
        recent.clear()
        recorder.enabled.set(false)
        stopped.set(true)
      }
    }
  }
}
