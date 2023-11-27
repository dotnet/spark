/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.deploy.dotnet

import org.apache.spark.SparkException

/**
 * This exception type describes an exception thrown by a .NET user application.
 *
 * @param exitCode         Exit code returned by the .NET application.
 * @param dotNetStackTrace Stacktrace extracted from .NET application logs.
 */
private[spark] class DotNetUserAppException(exitCode: Int, dotNetStackTrace: Option[String])
  extends SparkException(
    dotNetStackTrace match {
      case None => s"User application exited with $exitCode"
      case Some(e) => s"User application exited with $exitCode and .NET exception: $e"
    })
