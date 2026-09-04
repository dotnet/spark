/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */


package org.apache.spark.api.dotnet

import java.io.DataInputStream

private[dotnet] object Extensions {
  implicit class DataInputStreamExt(stream: DataInputStream) {
    def readNBytes(n: Int): Array[Byte] = {
      val buf = new Array[Byte](n)
      stream.readFully(buf)
      buf
    }
  }
}
