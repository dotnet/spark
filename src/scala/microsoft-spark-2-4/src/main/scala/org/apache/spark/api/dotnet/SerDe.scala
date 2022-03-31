/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.io.{DataInputStream, DataOutputStream}
import java.nio.charset.StandardCharsets
import java.sql.{Date, Time, Timestamp}

import org.apache.spark.sql.Row

import scala.collection.JavaConverters._

/**
 * Class responsible for serialization and deserialization between CLR & JVM.
 * This implementation of methods is mostly identical to the SerDe implementation in R.
 */
class SerDe(val tracker: JVMObjectTracker) {
  def readObjectType(dis: DataInputStream): Char = {
    dis.readByte().toChar
  }

  def readObject(dis: DataInputStream): Object = {
    val dataType = readObjectType(dis)
    readTypedObject(dis, dataType)
  }

  private def readTypedObject(dis: DataInputStream, dataType: Char): Object = {
    dataType match {
      case 'n' => null
      case 'i' => new java.lang.Integer(readInt(dis))
      case 'g' => new java.lang.Long(readLong(dis))
      case 'd' => new java.lang.Double(readDouble(dis))
      case 'b' => new java.lang.Boolean(readBoolean(dis))
      case 'c' => readString(dis)
      case 'e' => readMap(dis)
      case 'r' => readBytes(dis)
      case 'l' => readList(dis)
      case 'D' => readDate(dis)
      case 't' => readTime(dis)
      case 'j' => tracker.getObject(readString(dis))
      case 'R' => readRowArr(dis)
      case 'O' => readObjectArr(dis)
      case _ => throw new IllegalArgumentException(s"Invalid type $dataType")
    }
  }

  private def readBytes(in: DataInputStream): Array[Byte] = {
    val len = readInt(in)
    val out = new Array[Byte](len)
    in.readFully(out)
    out
  }

  def readInt(in: DataInputStream): Int = {
    in.readInt()
  }

  private def readLong(in: DataInputStream): Long = {
    in.readLong()
  }

  private def readDouble(in: DataInputStream): Double = {
    in.readDouble()
  }

  private def readStringBytes(in: DataInputStream, len: Int): String = {
    val bytes = new Array[Byte](len)
    in.readFully(bytes)
    val str = new String(bytes, "UTF-8")
    str
  }

  def readString(in: DataInputStream): String = {
    val len = in.readInt()
    readStringBytes(in, len)
  }

  def readBoolean(in: DataInputStream): Boolean = {
    in.readBoolean()
  }

  private def readDate(in: DataInputStream): Date = {
    Date.valueOf(readString(in))
  }

  private def readTime(in: DataInputStream): Timestamp = {
    val seconds = in.readDouble()
    val sec = Math.floor(seconds).toLong
    val t = new Timestamp(sec * 1000L)
    t.setNanos(((seconds - sec) * 1e9).toInt)
    t
  }

  private def readRow(in: DataInputStream): Row = {
    val len = readInt(in)
    Row.fromSeq((0 until len).map(_ => readObject(in)))
  }

  private def readBytesArr(in: DataInputStream): Array[Array[Byte]] = {
    val len = readInt(in)
    (0 until len).map(_ => readBytes(in)).toArray
  }

  private def readIntArr(in: DataInputStream): Array[Int] = {
    val len = readInt(in)
    (0 until len).map(_ => readInt(in)).toArray
  }

  private def readLongArr(in: DataInputStream): Array[Long] = {
    val len = readInt(in)
    (0 until len).map(_ => readLong(in)).toArray
  }

  private def readDoubleArr(in: DataInputStream): Array[Double] = {
    val len = readInt(in)
    (0 until len).map(_ => readDouble(in)).toArray
  }

  private def readDoubleArrArr(in: DataInputStream): Array[Array[Double]] = {
    val len = readInt(in)
    (0 until len).map(_ => readDoubleArr(in)).toArray
  }

  private def readBooleanArr(in: DataInputStream): Array[Boolean] = {
    val len = readInt(in)
    (0 until len).map(_ => readBoolean(in)).toArray
  }

  private def readStringArr(in: DataInputStream): Array[String] = {
    val len = readInt(in)
    (0 until len).map(_ => readString(in)).toArray
  }

  private def readRowArr(in: DataInputStream): java.util.List[Row] = {
    val len = readInt(in)
    (0 until len).map(_ => readRow(in)).toList.asJava
  }

  private def readObjectArr(in: DataInputStream): Seq[Any] = {
    val len = readInt(in)
    (0 until len).map(_ => readObject(in))
  }

  private def readList(dis: DataInputStream): Array[_] = {
    val arrType = readObjectType(dis)
    arrType match {
      case 'i' => readIntArr(dis)
      case 'g' => readLongArr(dis)
      case 'c' => readStringArr(dis)
      case 'd' => readDoubleArr(dis)
      case 'A' => readDoubleArrArr(dis)
      case 'b' => readBooleanArr(dis)
      case 'j' => readStringArr(dis).map(x => tracker.getObject(x))
      case 'r' => readBytesArr(dis)
      case _ => throw new IllegalArgumentException(s"Invalid array type $arrType")
    }
  }

  private def readMap(in: DataInputStream): java.util.Map[Object, Object] = {
    val len = readInt(in)
    if (len > 0) {
      val keysType = readObjectType(in)
      val keysLen = readInt(in)
      val keys = (0 until keysLen).map(_ => readTypedObject(in, keysType))

      val valuesLen = readInt(in)
      val values = (0 until valuesLen).map(_ => {
        val valueType = readObjectType(in)
        readTypedObject(in, valueType)
      })
      keys.zip(values).toMap.asJava
    } else {
      new java.util.HashMap[Object, Object]()
    }
  }

  // Using the same mapping as SparkR implementation for now
  // Methods to write out data from Java to .NET.
  //
  // Type mapping from Java to .NET:
  //
  // void -> NULL
  // Int -> integer
  // String -> character
  // Boolean -> logical
  // Float -> double
  // Double -> double
  // Long -> long
  // Array[Byte] -> raw
  // Date -> Date
  // Time -> POSIXct
  //
  // Array[T] -> list()
  // Object -> jobj

  def writeType(dos: DataOutputStream, typeStr: String): Unit = {
    typeStr match {
      case "void" => dos.writeByte('n')
      case "character" => dos.writeByte('c')
      case "double" => dos.writeByte('d')
      case "doublearray" => dos.writeByte('A')
      case "long" => dos.writeByte('g')
      case "integer" => dos.writeByte('i')
      case "logical" => dos.writeByte('b')
      case "date" => dos.writeByte('D')
      case "time" => dos.writeByte('t')
      case "raw" => dos.writeByte('r')
      case "list" => dos.writeByte('l')
      case "jobj" => dos.writeByte('j')
      case _ => throw new IllegalArgumentException(s"Invalid type $typeStr")
    }
  }

  def writeObject(dos: DataOutputStream, value: Object): Unit = {
    if (value == null || value == Unit) {
      writeType(dos, "void")
    } else {
      value.getClass.getName match {
        case "java.lang.String" =>
          writeType(dos, "character")
          writeString(dos, value.asInstanceOf[String])
        case "float" | "java.lang.Float" =>
          writeType(dos, "double")
          writeDouble(dos, value.asInstanceOf[Float].toDouble)
        case "double" | "java.lang.Double" =>
          writeType(dos, "double")
          writeDouble(dos, value.asInstanceOf[Double])
        case "long" | "java.lang.Long" =>
          writeType(dos, "long")
          writeLong(dos, value.asInstanceOf[Long])
        case "int" | "java.lang.Integer" =>
          writeType(dos, "integer")
          writeInt(dos, value.asInstanceOf[Int])
        case "boolean" | "java.lang.Boolean" =>
          writeType(dos, "logical")
          writeBoolean(dos, value.asInstanceOf[Boolean])
        case "java.sql.Date" =>
          writeType(dos, "date")
          writeDate(dos, value.asInstanceOf[Date])
        case "java.sql.Time" =>
          writeType(dos, "time")
          writeTime(dos, value.asInstanceOf[Time])
        case "java.sql.Timestamp" =>
          writeType(dos, "time")
          writeTime(dos, value.asInstanceOf[Timestamp])
        case "[B" =>
          writeType(dos, "raw")
          writeBytes(dos, value.asInstanceOf[Array[Byte]])
        // TODO: Types not handled right now include
        // byte, char, short, float

        // Handle arrays
        case "[Ljava.lang.String;" =>
          writeType(dos, "list")
          writeStringArr(dos, value.asInstanceOf[Array[String]])
        case "[I" =>
          writeType(dos, "list")
          writeIntArr(dos, value.asInstanceOf[Array[Int]])
        case "[J" =>
          writeType(dos, "list")
          writeLongArr(dos, value.asInstanceOf[Array[Long]])
        case "[D" =>
          writeType(dos, "list")
          writeDoubleArr(dos, value.asInstanceOf[Array[Double]])
        case "[[D" =>
          writeType(dos, "list")
          writeDoubleArrArr(dos, value.asInstanceOf[Array[Array[Double]]])
        case "[Z" =>
          writeType(dos, "list")
          writeBooleanArr(dos, value.asInstanceOf[Array[Boolean]])
        case "[[B" =>
          writeType(dos, "list")
          writeBytesArr(dos, value.asInstanceOf[Array[Array[Byte]]])
        case otherName =>
          // Handle array of objects
          if (otherName.startsWith("[L")) {
            val objArr = value.asInstanceOf[Array[Object]]
            writeType(dos, "list")
            writeType(dos, "jobj")
            dos.writeInt(objArr.length)
            objArr.foreach(o => writeJObj(dos, o))
          } else {
            writeType(dos, "jobj")
            writeJObj(dos, value)
          }
      }
    }
  }

  def writeInt(out: DataOutputStream, value: Int): Unit = {
    out.writeInt(value)
  }

  def writeLong(out: DataOutputStream, value: Long): Unit = {
    out.writeLong(value)
  }

  private def writeDouble(out: DataOutputStream, value: Double): Unit = {
    out.writeDouble(value)
  }

  private def writeBoolean(out: DataOutputStream, value: Boolean): Unit = {
    out.writeBoolean(value)
  }

  private def writeDate(out: DataOutputStream, value: Date): Unit = {
    writeString(out, value.toString)
  }

  private def writeTime(out: DataOutputStream, value: Time): Unit = {
    out.writeDouble(value.getTime.toDouble / 1000.0)
  }

  private def writeTime(out: DataOutputStream, value: Timestamp): Unit = {
    out.writeDouble((value.getTime / 1000).toDouble + value.getNanos.toDouble / 1e9)
  }

  def writeString(out: DataOutputStream, value: String): Unit = {
    val utf8 = value.getBytes(StandardCharsets.UTF_8)
    val len = utf8.length
    out.writeInt(len)
    out.write(utf8, 0, len)
  }

  private def writeBytes(out: DataOutputStream, value: Array[Byte]): Unit = {
    out.writeInt(value.length)
    out.write(value)
  }

  def writeJObj(out: DataOutputStream, value: Object): Unit = {
    val objId = tracker.put(value)
    writeString(out, objId)
  }

  private def writeIntArr(out: DataOutputStream, value: Array[Int]): Unit = {
    writeType(out, "integer")
    out.writeInt(value.length)
    value.foreach(v => out.writeInt(v))
  }

  private def writeLongArr(out: DataOutputStream, value: Array[Long]): Unit = {
    writeType(out, "long")
    out.writeInt(value.length)
    value.foreach(v => out.writeLong(v))
  }

  private def writeDoubleArr(out: DataOutputStream, value: Array[Double]): Unit = {
    writeType(out, "double")
    out.writeInt(value.length)
    value.foreach(v => out.writeDouble(v))
  }

  private def writeDoubleArrArr(out: DataOutputStream, value: Array[Array[Double]]): Unit = {
    writeType(out, "doublearray")
    out.writeInt(value.length)
    value.foreach(v => writeDoubleArr(out, v))
  }

  private def writeBooleanArr(out: DataOutputStream, value: Array[Boolean]): Unit = {
    writeType(out, "logical")
    out.writeInt(value.length)
    value.foreach(v => writeBoolean(out, v))
  }

  private def writeStringArr(out: DataOutputStream, value: Array[String]): Unit = {
    writeType(out, "character")
    out.writeInt(value.length)
    value.foreach(v => writeString(out, v))
  }

  private def writeBytesArr(out: DataOutputStream, value: Array[Array[Byte]]): Unit = {
    writeType(out, "raw")
    out.writeInt(value.length)
    value.foreach(v => writeBytes(out, v))
  }
}

private object SerializationFormats {
  val BYTE = "byte"
  val STRING = "string"
  val ROW = "row"
}
