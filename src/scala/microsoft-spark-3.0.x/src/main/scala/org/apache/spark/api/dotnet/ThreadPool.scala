/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.util.concurrent.{ExecutorService, Executors}

import scala.collection.mutable

/**
 * Pool of thread executors. There should be a 1-1 correspondence between C# threads
 * and Java threads.
 */
object ThreadPool {

  /**
   * Map from threadId to corresponding executor.
   */
  val executors: mutable.HashMap[Int, ExecutorService] =
    new mutable.HashMap[Int, ExecutorService]()

  /**
   * Run some code on a particular thread.
   *
   * @param threadId Integer id of the thread.
   * @param task Function to run on the thread.
   */
  def run(threadId: Int, task: () => Unit): Unit = {
    val executor = getOrCreateExecutor(threadId)
    val future = executor.submit(new Runnable {
      override def run(): Unit = task()
    })

    future.get()
  }

  /**
   * Delete a particular thread.
   *
   * @param threadId Integer id of the thread.
   */
  def deleteThread(threadId: Int): Unit = synchronized {
    executors.remove(threadId).foreach(_.shutdown)
  }

  /**
   * Get the executor if it exists, otherwise create a new one.
   *
   * @param id Integer id of the thread.
   * @return
   */
  private def getOrCreateExecutor(id: Int): ExecutorService = synchronized {
    executors.getOrElseUpdate(id, Executors.newSingleThreadExecutor)
  }
}
