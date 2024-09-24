/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */


package org.apache.spark.api.dotnet

import scala.collection.mutable.HashMap

/**
 * Tracks JVM objects returned to .NET which is useful for invoking calls from .NET on JVM objects.
 */
private[dotnet] class JVMObjectTracker {

  // Multiple threads may access objMap and increase objCounter. Because get method return Option,
  // it is convenient to use a Scala map instead of java.util.concurrent.ConcurrentHashMap.
  private[this] val objMap = new HashMap[String, Object]
  private[this] var objCounter: Int = 1

  def getObject(id: String): Object = {
    synchronized {
      objMap(id)
    }
  }

  def get(id: String): Option[Object] = {
    synchronized {
      objMap.get(id)
    }
  }

  def put(obj: Object): String = {
    synchronized {
      val objId = objCounter.toString
      objCounter = objCounter + 1
      objMap.put(objId, obj)
      objId
    }
  }

  def remove(id: String): Option[Object] = {
    synchronized {
      objMap.remove(id)
    }
  }

  def clear(): Unit = {
    synchronized {
      objMap.clear()
      objCounter = 1
    }
  }
}
