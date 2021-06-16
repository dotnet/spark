/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.util.concurrent.{ExecutorService, Executors}
import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * Pool of thread executors. There should be a 1-1 correspondence between C# threads
 * and Java threads.
 */
object ThreadPool extends Logging {

  /**
   * Map from (processId, threadId) to corresponding executor.
   */
  private val executors: mutable.HashMap[(Int, Int), ExecutorService] =
    new mutable.HashMap[(Int, Int), ExecutorService]()

  /**
   * Run some code on a particular thread.
   * @param processId Integer id of the process.
   * @param threadId Integer id of the thread.
   * @param task Function to run on the thread.
   */
  def run(processId: Int, threadId: Int, task: () => Unit): Unit = {
    logInfo(s"Calling ThreadPool.run() [process id = $processId, thread id = $threadId] ...")
    val executor = getOrCreateExecutor(processId, threadId)
    val future = executor.submit(new Runnable {
      override def run(): Unit = task()
    })

    future.get()
  }

  /**
   * Try to delete a particular thread.
   * @param processId Integer id of the process.
   * @param threadId Integer id of the thread.
   * @return True if successful, false if thread does not exist.
   */
  def tryDeleteThread(processId: Int, threadId: Int): Boolean = synchronized {
    logInfo(s"Calling ThreadPool.tryDeleteThread() [process id = $processId, thread id = $threadId] ...")
    executors.remove((processId, threadId)) match {
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
    logInfo(s"Calling ThreadPool.shutdown() ...")
    executors.foreach(_._2.shutdown())
    executors.clear()
  }

  /**
   * Get the executor if it exists, otherwise create a new one.
   * @param processId Integer id of the process.
   * @param threadId Integer id of the thread.
   * @return The new or existing executor with the given id.
   */
  private def getOrCreateExecutor(processId: Int, threadId: Int): ExecutorService = synchronized {
    logInfo(s"Calling ThreadPool.getOrCreateExecutor() [process id = $processId, thread id = $threadId] ...")
    executors.getOrElseUpdate((processId, threadId), Executors.newSingleThreadExecutor)
  }
}
