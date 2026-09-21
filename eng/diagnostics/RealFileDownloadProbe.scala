/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark

import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.util.concurrent.{ConcurrentHashMap, ExecutionException, Executors, ThreadFactory, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.config.Configurator

import org.apache.spark.rpc.RpcEnv

/** Standalone diagnostic using Spark's real missing-stream response, callback and Pipe. */
object RealFileDownloadProbe {
  private class Reader(val request: Int, val mode: String) {
    val started: Long = System.nanoTime()
    @volatile var phase: String = "opening"
    @volatile var channel: ReadableByteChannel = _
  }

  private val active = new ConcurrentHashMap[Thread, Reader]()
  private val stopping = new AtomicBoolean()
  private val unix = new AtomicInteger()
  private val inet = new AtomicInteger()
  private val unknown = new AtomicInteger()
  private val requests = new AtomicInteger()

  private def field(target: AnyRef, name: String): AnyRef = {
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

  private def family(channel: ReadableByteChannel): Int = {
    val source = field(channel, "source")
    val socket = field(source, "sc")
    field(socket, "family").toString match {
      case "UNIX" => 1
      case "INET" | "INET6" => 2
      case _ => 0
    }
  }

  // Never print exception messages, paths, stream URIs, environment values or payloads.
  private def symbol(value: String): String = {
    if (value.matches("[A-Za-z0-9_.$<>]+")) value else "other"
  }

  private def diagnosticError(error: Throwable): Unit = {
    System.out.println(s"probe_diagnostic_error type=${symbol(error.getClass.getName)}")
  }

  private def stop(reason: String, code: Int): Nothing = {
    if (stopping.compareAndSet(false, true)) {
      try {
        System.out.println(s"probe_stop reason=$reason active=${active.size()}")
        active.entrySet().asScala.take(8).foreach { entry =>
          val reader = entry.getValue
          val elapsed = (System.nanoTime() - reader.started) / 1000000
          val prefix = s"reader id=${entry.getKey.getId} request=${reader.request} " +
            s"mode=${reader.mode} phase=${reader.phase}"
          // These are non-atomic Java observations, not proof of native sink closure.
          // The sink is owned by Spark's callback and is intentionally not intercepted.
          try {
            val channel = reader.channel
            if (channel == null) {
              System.out.println(s"$prefix elapsed_ms=$elapsed")
            } else {
              System.out.println(s"$prefix open=${channel.isOpen} " +
                s"source_error=${field(channel, "error") != null} " +
                s"family=${family(channel)} elapsed_ms=$elapsed")
            }
          } catch { case NonFatal(error) =>
            System.out.println(s"$prefix elapsed_ms=$elapsed")
            diagnosticError(error)
          }
          // A failed reflection sample must not suppress the blocked reader's stack.
          try {
            entry.getKey.getStackTrace.take(32).foreach { frame =>
              System.out.println(s"frame=${symbol(frame.getClassName)}." +
                s"${symbol(frame.getMethodName)} line=${frame.getLineNumber}")
            }
          } catch { case NonFatal(error) => diagnosticError(error) }
        }
      } finally {
        // The parent runner separately bounds this child JVM, including shutdown hooks.
        System.exit(code)
      }
    }
    throw new IllegalStateException("probe-stopping")
  }

  private def runRequest(owner: RpcEnv, baseUri: String, mode: String): Unit = {
    val reader = new Reader(requests.incrementAndGet(), mode)
    val thread = Thread.currentThread()
    active.put(thread, reader)
    try {
      val streamId = s"/files/missing-probe-${reader.request}.class"
      // No bridge JAR, .NET process, callback replacement, or artificial close.
      // NettyStreamManager returns null for this unregistered file, causing the real
      // TransportRequestHandler to send StreamFailure and Spark to close its Pipe sink.
      val source = owner.openChannel(baseUri + streamId)
      reader.channel = source
      if (mode == "read_after_error") {
        reader.phase = "waiting_error"
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(4)
        while (field(source, "error") == null && System.nanoTime() < deadline) {
          Thread.sleep(1)
        }
        require(field(source, "error") != null, "missing failure callback")
      }
      // immediate_read does not force a read-before-callback schedule. Conversely,
      // observing source.error does not establish that sink.close has completed.
      reader.phase = "reading"
      var observed: Throwable = null
      try source.read(ByteBuffer.allocate(1))
      catch { case NonFatal(error) => observed = error }
      reader.phase = "validating"
      // Missing-file failure is expected. EOF, another exception, a timeout-induced
      // close, or a merely similar exception must never count as a successful probe.
      require(observed != null && (observed eq field(source, "error")),
        "expected original missing-file exception")
      require(observed.getClass == classOf[RuntimeException] &&
        observed.getMessage == s"Stream '$streamId' was not found.",
        "expected synthetic missing-stream response")
      family(source) match {
        case 1 => unix.incrementAndGet()
        case 2 => inet.incrementAndGet()
        case _ => unknown.incrementAndGet()
      }
    } finally {
      // Only the reader closes its own source, after read returns/throws. Neither
      // the observer nor timeout code closes channels to manufacture completion.
      reader.phase = "closing"
      if (reader.channel != null) reader.channel.close()
      active.remove(thread)
    }
  }

  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.OFF)
    val parsed = try {
      require(args.length == 3 && args(0).matches("4\\.0\\.[0-9]+"))
      val iterations = args(1).toInt
      val concurrency = args(2).toInt
      require(iterations >= 1 && iterations <= 10000)
      require(concurrency >= 1 && concurrency <= 8)
      (args(0), iterations, concurrency)
    } catch { case NonFatal(_) => stop("invalid_arguments", 5) }
    val (expectedVersion, iterations, concurrency) = parsed
    if (SPARK_VERSION != expectedVersion || Runtime.version().feature() != 17 ||
        !System.getProperty("os.name", "").startsWith("Windows")) {
      stop("runtime_mismatch", 5)
    }
    System.out.println(s"probe_start spark=$SPARK_VERSION java_feature=17 " +
      s"iterations=$iterations concurrency=$concurrency")

    val watchdog = new Thread(() => {
      Thread.sleep(120000)
      stop("process_deadline", 4)
    }, "probe-deadline")
    watchdog.setDaemon(true)
    watchdog.start()

    try {
      val conf = new SparkConf(false)
        .set("spark.rpc.io.threads", "1")
        .set("spark.rpc.netty.dispatcher.numThreads", "1")
      val owner = RpcEnv.create("real-file-download-probe", "127.0.0.1", 0,
        conf, new SecurityManager(conf), clientMode = false)
      val readers = Executors.newFixedThreadPool(concurrency, new ThreadFactory {
        override def newThread(action: Runnable): Thread = {
          val thread = new Thread(action, "probe-reader")
          thread.setDaemon(true)
          thread
        }
      })
      try {
        val baseUri = owner.address.toSparkURL
        for (mode <- Seq("immediate_read", "read_after_error")) {
          var completed = 0
          val started = System.nanoTime()
          while (completed < iterations) {
            val count = math.min(concurrency, iterations - completed)
            // One absolute batch budget includes openChannel and every reader;
            // sequential Future.get calls must not each start another full timeout.
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
            val futures = (0 until count).map { _ => readers.submit(new Runnable {
              override def run(): Unit = runRequest(owner, baseUri, mode)
            }) }
            futures.foreach { future =>
              try {
                val remaining = deadline - System.nanoTime()
                if (remaining <= 0) stop("batch_deadline", 2)
                future.get(remaining, TimeUnit.NANOSECONDS)
              } catch {
                case _: TimeoutException => stop("batch_deadline", 2)
                case error: ExecutionException =>
                  System.out.println(s"probe_operation_error " +
                    s"type=${symbol(error.getCause.getClass.getName)}")
                  stop("unexpected_outcome", 3)
              }
            }
            completed += count
          }
          System.out.println(s"probe_result mode=$mode completed_expected_errors=$completed " +
            s"concurrency=$concurrency elapsed_ms=${(System.nanoTime() - started) / 1000000}")
        }
      } finally {
        readers.shutdown()
        owner.shutdown()
        owner.awaitTermination()
      }
      if (unknown.get() != 0 || active.size() != 0 ||
          unix.get() + inet.get() != iterations * 2) {
        stop("diagnostic_failure", 6)
      }
      System.out.println(s"probe_complete unix=${unix.get()} inet=${inet.get()} " +
        s"unknown=${unknown.get()} active=${active.size()}")
    } catch { case NonFatal(error) =>
      System.out.println(s"probe_operation_error type=${symbol(error.getClass.getName)}")
      stop("setup_or_cleanup_failure", 3)
    }
  }
}
