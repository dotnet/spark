/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.sql.api.dotnet

import org.apache.spark.SparkConf

/*
 * Utils for JvmBridge.
 */
object JvmBridgeUtils {
  def getKeyValuePairAsString(kvp: (String, String)): String = {
    return kvp._1 + "=" + kvp._2
  }

  def getKeyValuePairArrayAsString(kvpArray: Array[(String, String)]): String = {
    val sb = new StringBuilder

    for (kvp <- kvpArray) {
      sb.append(getKeyValuePairAsString(kvp))
      sb.append(";")
    }

    sb.toString
  }

  def getSparkConfAsString(sparkConf: SparkConf): String = {
    getKeyValuePairArrayAsString(sparkConf.getAll)
  }
}
