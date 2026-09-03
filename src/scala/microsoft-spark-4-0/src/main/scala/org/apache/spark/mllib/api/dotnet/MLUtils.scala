
/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.mllib.api.dotnet

import org.apache.spark.ml._
import scala.collection.JavaConverters._

/** MLUtils object that hosts helper functions
  * related to ML usage
  */
object MLUtils {

  /** A helper function to let pipeline accept java.util.ArrayList
    * format stages in scala code
    * @param pipeline - The pipeline to be set stages
    * @param value - A java.util.ArrayList of PipelineStages to be set as stages
    * @return The pipeline
    */
  def setPipelineStages(pipeline: Pipeline, value: java.util.ArrayList[_ <: PipelineStage]): Pipeline =
    pipeline.setStages(value.asScala.toArray)
}
