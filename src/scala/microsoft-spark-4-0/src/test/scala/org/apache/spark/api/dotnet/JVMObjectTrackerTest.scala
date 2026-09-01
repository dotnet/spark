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
    val tracker = new JVMObjectTracker
    val firstId = tracker.put(new Object)
    val secondId = tracker.put(new Object)
    val thirdId = tracker.put(new Object)

    tracker.clear()

    assert(tracker.get(firstId).isEmpty)
    assert(tracker.get(secondId).isEmpty)
    assert(tracker.get(thirdId).isEmpty)
  }

  @Test
  def shouldResetCounter(): Unit = {
    val tracker = new JVMObjectTracker
    val firstId = tracker.put(new Object)
    val secondId = tracker.put(new Object)

    tracker.clear()

    val thirdId = tracker.put(new Object)

    assert(firstId.equals("1"))
    assert(secondId.equals("2"))
    assert(thirdId.equals("1"))
  }
}
