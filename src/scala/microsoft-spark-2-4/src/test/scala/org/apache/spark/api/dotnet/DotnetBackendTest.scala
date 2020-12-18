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
