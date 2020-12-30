/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import org.junit.{Ignore, Test}

@Test
class DotnetBackendTest extends {

  @Ignore
  def shouldReleaseJVMReferencesWhenClose(): Unit = {
    val backend = new DotnetBackend
    val tracker = new JVMObjectTracker
    val objectId = tracker.put(new Object)

    backend.close()

    assert(
      tracker.get(objectId).isEmpty,
      "JVMObjectTracker must be cleaned up during backend shutdown.")
  }
}
