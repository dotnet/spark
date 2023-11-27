/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import org.apache.spark.SparkContext
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.python._
import org.apache.spark.rdd.RDD

object DotnetRDD {
  def createPythonRDD(
      parent: RDD[_],
      func: PythonFunction,
      preservePartitoning: Boolean): PythonRDD = {
    new PythonRDD(parent, func, preservePartitoning)
  }

  def createJavaRDDFromArray(
      sc: SparkContext,
      arr: Array[Array[Byte]],
      numSlices: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(sc.parallelize(arr, numSlices))
  }

  def toJavaRDD(rdd: RDD[_]): JavaRDD[_] = JavaRDD.fromRDD(rdd)
}
