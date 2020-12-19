/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import org.junit.Test

@Test
class DotnetBackendTest {

  @Test
  def shouldReleaseJVMReferencesWhenClose(): Unit = {
    val backend = new DotnetBackend
    val objectId = JVMObjectTracker.put(new Object)

    backend.close()

    assert(
      JVMObjectTracker.get(objectId).isEmpty,
      "JVMObjectTracker must be cleaned up during backend shutdown.")
  }
}
