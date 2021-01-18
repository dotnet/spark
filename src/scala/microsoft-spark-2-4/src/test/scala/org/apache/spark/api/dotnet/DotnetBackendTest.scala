/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import org.junit.Assert._
import org.junit.function.ThrowingRunnable
import org.junit.{After, Before, Test}

import java.net.InetAddress

@Test
class DotnetBackendTest {
  private var backend: DotnetBackend = _

  @Before
  def before(): Unit = {
    backend = new DotnetBackend
  }

  @After
  def after(): Unit = {
    backend.close()
  }

  @Test
  def shouldNotResetCallbackClient(): Unit = {
    // Specifying port = 0 to select port dynamically.
    backend.setCallbackClient(InetAddress.getLoopbackAddress.toString, port = 0)

    assertTrue(backend.callbackClient.isDefined)
    assertThrows(
      classOf[Exception],
      new ThrowingRunnable {
        override def run(): Unit = {
          backend.setCallbackClient(InetAddress.getLoopbackAddress.toString, port = 0)
        }
      })
  }
}
