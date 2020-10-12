/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelFuture, ChannelInitializer, EventLoopGroup}
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Dotnet.DOTNET_NUM_BACKEND_THREADS
import org.apache.spark.{SparkConf, SparkEnv}

/**
 * Netty server that invokes JVM calls based upon receiving messages from .NET.
 * The implementation mirrors the RBackend.
 *
 */
class DotnetBackend extends Logging {
  self => // for accessing the this reference in inner class(ChannelInitializer)
  private[this] var channelFuture: ChannelFuture = _
  private[this] var bootstrap: ServerBootstrap = _
  private[this] var bossGroup: EventLoopGroup = _

  def init(portNumber: Int): Int = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    val numBackendThreads = conf.get(DOTNET_NUM_BACKEND_THREADS)
    logInfo(s"The number of DotnetBackend threads is set to $numBackendThreads.")
    bossGroup = new NioEventLoopGroup(numBackendThreads)
    val workerGroup = bossGroup

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])

    bootstrap.childHandler(new ChannelInitializer[SocketChannel]() {
      def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline()
          .addLast("encoder", new ByteArrayEncoder())
          .addLast(
            "frameDecoder",
            // maxFrameLength = 2G
            // lengthFieldOffset = 0
            // lengthFieldLength = 4
            // lengthAdjustment = 0
            // initialBytesToStrip = 4, i.e.  strip out the length field itself
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
          .addLast("decoder", new ByteArrayDecoder())
          .addLast("handler", new DotnetBackendHandler(self))
      }
    })

    channelFuture = bootstrap.bind(new InetSocketAddress("localhost", portNumber))
    channelFuture.syncUninterruptibly()
    channelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort
  }

  def run(): Unit = {
    channelFuture.channel.closeFuture().syncUninterruptibly()
  }

  def close(): Unit = {
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS)
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.config().group() != null) {
      bootstrap.config().group().shutdownGracefully()
    }
    if (bootstrap != null && bootstrap.config().childGroup() != null) {
      bootstrap.config().childGroup().shutdownGracefully()
    }
    bootstrap = null

    // Send close to .NET callback server.
    DotnetBackend.shutdownCallbackClient()

    // Shutdown the thread pool whose executors could still be running.
    ThreadPool.shutdown()
  }
}

object DotnetBackend extends Logging {
  @volatile private[spark] var callbackClient: CallbackClient = null

  private[spark] def setCallbackClient(address: String, port: Int) = synchronized {
    if (DotnetBackend.callbackClient == null) {
      logInfo(s"Connecting to a callback server at $address:$port")
      DotnetBackend.callbackClient = new CallbackClient(address, port)
    } else {
      throw new Exception("Callback client already set.")
    }
  }

  private[spark] def shutdownCallbackClient(): Unit = synchronized {
    if (callbackClient != null) {
      callbackClient.shutdown()
      callbackClient = null
    }
  }
}
