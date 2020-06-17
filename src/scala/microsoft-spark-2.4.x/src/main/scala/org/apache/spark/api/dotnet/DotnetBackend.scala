/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue, Phaser, TimeUnit}

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelFuture, ChannelInitializer, EventLoopGroup}
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}
import org.apache.spark.internal.Logging

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
    // need at least 3 threads, use 10 here for safety
    bossGroup = new NioEventLoopGroup(10)
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
    DotnetBackend.getActiveCallbackClient().shutdown()
  }
}

object DotnetBackend {
  private[this] val phaser = new Phaser(1)
  private[this] var callbackClient: CallbackClient = null

  // flag to denote whether the callback client is shutdown explicitly
  @volatile private[spark] var callbackClientShutdown: Boolean = false

  private[spark] def setActiveCallbackClient(client: CallbackClient): Unit = synchronized {
    if (callbackClientShutdown) {
      return
    }

    try {
      if (callbackClient != null) {
        phaser.register()
        callbackClient.shutdown()
      }

      callbackClient = client
    } finally {
      phaser.arriveAndDeregister()
    }
  }

  private[spark] def getActiveCallbackClient(): CallbackClient = {
    phaser.arriveAndAwaitAdvance()
    callbackClient
  }

  private[spark] def shutdownCallbackClient(): Unit = synchronized {
    if (callbackClientShutdown || callbackClient == null) {
      callbackClientShutdown = true
      return
    }

    callbackClient.shutdown()
  }
}
