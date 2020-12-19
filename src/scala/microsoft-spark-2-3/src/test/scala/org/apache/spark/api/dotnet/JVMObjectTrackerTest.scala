/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import org.junit.Test

@Test
class JVMObjectTrackerTest {

  @Test
  def shouldReleaseAllReferences(): Unit = {
    val firstId = JVMObjectTracker.put(new Object)
    val secondId = JVMObjectTracker.put(new Object)
    val thirdId = JVMObjectTracker.put(new Object)

    JVMObjectTracker.clear()

    assert(JVMObjectTracker.get(firstId).isEmpty)
    assert(JVMObjectTracker.get(secondId).isEmpty)
    assert(JVMObjectTracker.get(thirdId).isEmpty)
  }

  @Test
  def shouldResetCounter(): Unit = {
    val firstId = JVMObjectTracker.put(new Object)
    val secondId = JVMObjectTracker.put(new Object)

    JVMObjectTracker.clear()

    val thirdId = JVMObjectTracker.put(new Object)

    assert(firstId.equals("1"))
    assert(secondId.equals("2"))
    assert(thirdId.equals("1"))
  }
}
