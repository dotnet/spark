package org.apache.spark.api.dotnet

import java.util.concurrent.{ExecutorService, Executors, Future}

import scala.collection.mutable

/**
 * Pool of thread executors. There should be a 1-1 correspondence between C# threads
 * and Java threads.
 */
object ThreadPool {

  /**
   * Map from threadId to corresponding executor.
   */
  val executors: mutable.Map[Int, ExecutorService] = mutable.Map()

  /**
   * Run some code on a particular thread.
   *
   * @param threadId
   * @param task
   * @return
   */
  def run(threadId: Int, task: () => Unit): Future[_] =
    getOrCreateExecutor(threadId).submit(new Runnable {
      override def run(): Unit = task
    })

  /**
   * Delete a particular thread.
   *
   * @param threadId
   */
  def deleteThread(threadId: Int) = {
    getOrCreateExecutor(threadId).shutdown()
    executors.remove(threadId)
  }

  /**
   * Get the executor if it exists, otherwise create a new one.
   *
   * @param id
   * @return
   */
  private def getOrCreateExecutor(id: Int): ExecutorService =
    executors.getOrElse(id, {
      val thread = Executors.newSingleThreadExecutor()
      executors.put(id, thread)
      thread
    })
}
