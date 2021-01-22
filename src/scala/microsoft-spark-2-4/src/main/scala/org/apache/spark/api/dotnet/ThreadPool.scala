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
  private val executors: mutable.HashMap[String, ExecutorService] =
    new mutable.HashMap[String, ExecutorService]()

  /**
   * Run some code on a particular thread.
   *
   * @param threadId String id of the thread.
   * @param task Function to run on the thread.
   */
  def run(threadId: String, task: () => Unit): Unit = {
    val executor = getOrCreateExecutor(threadId)
    val future = executor.submit(new Runnable {
      override def run(): Unit = task()
    })

    future.get()
  }

  /**
   * Try to delete a particular thread.
   *
   * @param threadId String id of the thread.
   * @return True if successful, false if thread does not exist.
   */
  def tryDeleteThread(threadId: String): Boolean = synchronized {
    executors.remove(threadId) match {
      case Some(executorService) =>
        executorService.shutdown()
        true
      case None => false
    }
  }

  /**
   * Shutdown any running ExecutorServices.
   */
  def shutdown(): Unit = synchronized {
    executors.foreach(_._2.shutdown())
    executors.clear()
  }

  /**
   * Get the executor if it exists, otherwise create a new one.
   *
   * @param id String id of the thread.
   * @return The new or existing executor with the given id.
   */
  private def getOrCreateExecutor(id: String): ExecutorService = synchronized {
    executors.getOrElseUpdate(id, Executors.newSingleThreadExecutor)
  }
}
