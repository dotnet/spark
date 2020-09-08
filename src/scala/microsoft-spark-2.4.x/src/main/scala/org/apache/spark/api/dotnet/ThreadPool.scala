package org.apache.spark.api.dotnet

import java.util.concurrent.{ExecutorService, Executors}

import scala.collection._

/**
 * Pool of thread executors. There should be a 1-1 correspondence between C# threads
 * and Java threads.
 */
object ThreadPool {

  /**
   * Map from threadId to corresponding executor.
   */
  val executors: concurrent.TrieMap[Int, ExecutorService] =
    new concurrent.TrieMap[Int, ExecutorService]()

  /**
   * Run some code on a particular thread.
   *
   * @param threadId
   * @param task
   */
  def run(threadId: Int, task: () => Unit): Unit = {
    val runnable = new Runnable {
      override def run(): Unit = task()
    }
    val future = getOrCreateExecutor(threadId).submit(runnable)
    while (!future.isDone) {
      Thread.sleep(1000)
    }
  }

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
