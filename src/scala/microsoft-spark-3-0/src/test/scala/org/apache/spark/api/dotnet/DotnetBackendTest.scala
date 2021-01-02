/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */


package org.apache.spark.api.dotnet

import org.junit.Assert._
import org.junit.{After, Before, Test}

import java.net.InetAddress

@Test
class DotnetBackendTest {
  private var sut: DotnetBackend = _

  @Before
  def before(): Unit = {
    sut = new DotnetBackend
  }

  @After
  def after(): Unit = {
    sut.close()
  }

  @Test
  def shouldNotResetCallbackClient(): Unit = {
    // specifying port = 0 to select port dynamically
    sut.setCallbackClient(InetAddress.getLoopbackAddress.toString, port = 0)

    assertTrue(sut.callbackClient.isDefined)
    assertThrows(classOf[Exception], () => {
      sut.setCallbackClient(InetAddress.getLoopbackAddress.toString, port = 0)
    })
  }
}
