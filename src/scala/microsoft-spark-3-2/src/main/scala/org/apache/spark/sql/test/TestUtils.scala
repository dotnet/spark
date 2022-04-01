/*	
 * Licensed to the .NET Foundation under one or more agreements.	
 * The .NET Foundation licenses this file to you under the MIT license.	
 * See the LICENSE file in the project root for more information.	
 */

package org.apache.spark.sql.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.MemoryStream

object TestUtils {

  /**
   * Helper method to create typed MemoryStreams intended for use in unit tests.	
   * @param sqlContext The SQLContext.
   * @param streamType The type of memory stream to create. This string is the `Name`
   *                   property of the dotnet type.
   * @return A typed MemoryStream.
   */
  def createMemoryStream(implicit sqlContext: SQLContext, streamType: String): MemoryStream[_] = {
    import sqlContext.implicits._

    streamType match {
      case "Int32" => MemoryStream[Int]
      case "String" => MemoryStream[String]
      case _ => throw new Exception(s"$streamType not supported")
    }
  }
}
