package org.apache.spark.api.dotnet

import java.io.DataInputStream

object Extensions {
  implicit class DataInputStreamExt(stream: DataInputStream) {
    def readN(n: Int): Array[Byte] = {
      val buf = new Array[Byte](n)
      stream.readFully(buf)
      buf
    }
  }
}
