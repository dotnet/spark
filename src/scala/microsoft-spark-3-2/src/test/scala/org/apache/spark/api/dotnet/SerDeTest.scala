/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import org.apache.spark.api.dotnet.Extensions._
import org.apache.spark.sql.Row
import org.junit.Assert._
import org.junit.{Before, Test}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.sql.Date
import scala.collection.JavaConverters._

@Test
class SerDeTest {
  private var serDe: SerDe = _
  private var tracker: JVMObjectTracker = _

  @Before
  def before(): Unit = {
    tracker = new JVMObjectTracker
    serDe = new SerDe(tracker)
  }

  @Test
  def shouldReadNull(): Unit = {
    val input = givenInput(in => {
      in.writeByte('n')
    })

    assertEquals(null, serDe.readObject(input))
  }

  @Test
  def shouldThrowForUnsupportedTypes(): Unit = {
    val input = givenInput(in => {
      in.writeByte('_')
    })

    assertThrows(classOf[IllegalArgumentException], () => {
      serDe.readObject(input)
    })
  }

  @Test
  def shouldReadInteger(): Unit = {
    val input = givenInput(in => {
      in.writeByte('i')
      in.writeInt(42)
    })

    assertEquals(42, serDe.readObject(input))
  }

  @Test
  def shouldReadLong(): Unit = {
    val input = givenInput(in => {
      in.writeByte('g')
      in.writeLong(42)
    })

    assertEquals(42L, serDe.readObject(input))
  }

  @Test
  def shouldReadDouble(): Unit = {
    val input = givenInput(in => {
      in.writeByte('d')
      in.writeDouble(42.42)
    })

    assertEquals(42.42, serDe.readObject(input))
  }

  @Test
  def shouldReadBoolean(): Unit = {
    val input = givenInput(in => {
      in.writeByte('b')
      in.writeBoolean(true)
    })

    assertEquals(true, serDe.readObject(input))
  }

  @Test
  def shouldReadString(): Unit = {
    val payload = "Spark Dotnet"
    val input = givenInput(in => {
      in.writeByte('c')
      in.writeInt(payload.getBytes("UTF-8").length)
      in.write(payload.getBytes("UTF-8"))
    })

    assertEquals(payload, serDe.readObject(input))
  }

  @Test
  def shouldReadMap(): Unit = {
    val input = givenInput(in => {
      in.writeByte('e') // map type descriptor
      in.writeInt(3) // size
      in.writeByte('i') // key type
      in.writeInt(3) // number of keys
      in.writeInt(11) // first key
      in.writeInt(22) // second key
      in.writeInt(33) // third key
      in.writeInt(3) // number of values
      in.writeByte('b') // first value type
      in.writeBoolean(true) // first value
      in.writeByte('d') // second value type
      in.writeDouble(42.42) // second value
      in.writeByte('n') // third type & value
    })

    assertEquals(
      mapAsJavaMap(Map(
        11 -> true,
        22 -> 42.42,
        33 -> null)),
      serDe.readObject(input))
  }

  @Test
  def shouldReadEmptyMap(): Unit = {
    val input = givenInput(in => {
      in.writeByte('e') // map type descriptor
      in.writeInt(0) // size
    })

    assertEquals(mapAsJavaMap(Map()), serDe.readObject(input))
  }

  @Test
  def shouldReadBytesArray(): Unit = {
    val input = givenInput(in => {
      in.writeByte('r') // byte array type descriptor
      in.writeInt(3) // length
      in.write(Array[Byte](1, 2, 3)) // payload
    })

    assertArrayEquals(Array[Byte](1, 2, 3), serDe.readObject(input).asInstanceOf[Array[Byte]])
  }

  @Test
  def shouldReadEmptyBytesArray(): Unit = {
    val input = givenInput(in => {
      in.writeByte('r') // byte array type descriptor
      in.writeInt(0) // length
    })

    assertArrayEquals(Array[Byte](), serDe.readObject(input).asInstanceOf[Array[Byte]])
  }

  @Test
  def shouldReadEmptyList(): Unit = {
    val input = givenInput(in => {
      in.writeByte('l') // type descriptor
      in.writeByte('i') // element type
      in.writeInt(0) // length
    })

    assertArrayEquals(Array[Int](), serDe.readObject(input).asInstanceOf[Array[Int]])
  }

  @Test
  def shouldReadList(): Unit = {
    val input = givenInput(in => {
      in.writeByte('l') // type descriptor
      in.writeByte('b') // element type
      in.writeInt(3) // length
      in.writeBoolean(true)
      in.writeBoolean(false)
      in.writeBoolean(true)
    })

    assertArrayEquals(Array(true, false, true), serDe.readObject(input).asInstanceOf[Array[Boolean]])
  }

  @Test
  def shouldThrowWhenReadingListWithUnsupportedType(): Unit = {
    val input = givenInput(in => {
      in.writeByte('l') // type descriptor
      in.writeByte('_') // unsupported element type
    })

    assertThrows(classOf[IllegalArgumentException], () => {
      serDe.readObject(input)
    })
  }

  @Test
  def shouldReadDate(): Unit = {
    val input = givenInput(in => {
      val date = "2020-12-31"
      in.writeByte('D') // type descriptor
      in.writeInt(date.getBytes("UTF-8").length) // date string size
      in.write(date.getBytes("UTF-8"))
    })

    assertEquals(Date.valueOf("2020-12-31"), serDe.readObject(input))
  }

  @Test
  def shouldReadObject(): Unit = {
    val trackingObject = new Object
    tracker.put(trackingObject)
    val input = givenInput(in => {
      val objectIndex = "1"
      in.writeByte('j') // type descriptor
      in.writeInt(objectIndex.getBytes("UTF-8").length) // size
      in.write(objectIndex.getBytes("UTF-8"))
    })

    assertSame(trackingObject, serDe.readObject(input))
  }

  @Test
  def shouldThrowWhenReadingNonTrackingObject(): Unit = {
    val input = givenInput(in => {
      val objectIndex = "42"
      in.writeByte('j') // type descriptor
      in.writeInt(objectIndex.getBytes("UTF-8").length) // size
      in.write(objectIndex.getBytes("UTF-8"))
    })

    assertThrows(classOf[NoSuchElementException], () => {
      serDe.readObject(input)
    })
  }

  @Test
  def shouldReadSparkRows(): Unit = {
    val input = givenInput(in => {
      in.writeByte('R') // type descriptor
      in.writeInt(2) // number of rows
      in.writeInt(1) // number of elements in 1st row
      in.writeByte('i') // type of 1st element in 1st row
      in.writeInt(11)
      in.writeInt(3) // number of elements in 2st row
      in.writeByte('b') // type of 1st element in 2nd row
      in.writeBoolean(true)
      in.writeByte('d') // type of 2nd element in 2nd row
      in.writeDouble(42.24)
      in.writeByte('g') // type of 3nd element in 2nd row
      in.writeLong(99)
    })

    assertEquals(
      seqAsJavaList(Seq(
        Row.fromSeq(Seq(11)),
        Row.fromSeq(Seq(true, 42.24, 99)))),
      serDe.readObject(input))
  }

  @Test
  def shouldReadArrayOfObjects(): Unit = {
    val input = givenInput(in => {
      in.writeByte('O') // type descriptor
      in.writeInt(2) // number of elements
      in.writeByte('i') // type of 1st element
      in.writeInt(42)
      in.writeByte('b') // type of 2nd element
      in.writeBoolean(true)
    })

    assertEquals(Seq(42, true), serDe.readObject(input).asInstanceOf[Seq[Any]])
  }

  @Test
  def shouldWriteNull(): Unit = {
    val in = whenOutput(out => {
      serDe.writeObject(out, null)
      serDe.writeObject(out, Unit)
    })

    assertEquals(in.readByte(), 'n')
    assertEquals(in.readByte(), 'n')
    assertEndOfStream(in)
  }

  @Test
  def shouldWriteString(): Unit = {
    val sparkDotnet = "Spark Dotnet"
    val in = whenOutput(out => {
      serDe.writeObject(out, sparkDotnet)
    })

    assertEquals(in.readByte(), 'c') // object type
    assertEquals(in.readInt(), sparkDotnet.length) // length
    assertArrayEquals(in.readNBytes(sparkDotnet.length), sparkDotnet.getBytes("UTF-8"))
    assertEndOfStream(in)
  }

  @Test
  def shouldWritePrimitiveTypes(): Unit = {
    val in = whenOutput(out => {
      serDe.writeObject(out, 42.24f.asInstanceOf[Object])
      serDe.writeObject(out, 42L.asInstanceOf[Object])
      serDe.writeObject(out, 42.asInstanceOf[Object])
      serDe.writeObject(out, true.asInstanceOf[Object])
    })

    assertEquals(in.readByte(), 'd')
    assertEquals(in.readDouble(), 42.24F, 0.000001)
    assertEquals(in.readByte(), 'g')
    assertEquals(in.readLong(), 42L)
    assertEquals(in.readByte(), 'i')
    assertEquals(in.readInt(), 42)
    assertEquals(in.readByte(), 'b')
    assertEquals(in.readBoolean(), true)
    assertEndOfStream(in)
  }

  @Test
  def shouldWriteDate(): Unit = {
    val date = "2020-12-31"
    val in = whenOutput(out => {
      serDe.writeObject(out, Date.valueOf(date))
    })

    assertEquals(in.readByte(), 'D') // type
    assertEquals(in.readInt(), 10) // size
    assertArrayEquals(in.readNBytes(10), date.getBytes("UTF-8")) // content
  }

  @Test
  def shouldWriteCustomObjects(): Unit = {
    val customObject = new Object
    val in = whenOutput(out => {
      serDe.writeObject(out, customObject)
    })

    assertEquals(in.readByte(), 'j')
    assertEquals(in.readInt(), 1)
    assertArrayEquals(in.readNBytes(1), "1".getBytes("UTF-8"))
    assertSame(tracker.get("1").get, customObject)
  }

  @Test
  def shouldWriteArrayOfCustomObjects(): Unit = {
    val payload = Array(new Object, new Object)
    val in = whenOutput(out => {
      serDe.writeObject(out, payload)
    })

    assertEquals(in.readByte(), 'l') // array type
    assertEquals(in.readByte(), 'j') // type of element in array
    assertEquals(in.readInt(), 2) // array length
    assertEquals(in.readInt(), 1) // size of 1st element's identifiers
    assertArrayEquals(in.readNBytes(1), "1".getBytes("UTF-8")) // identifier of 1st element
    assertEquals(in.readInt(), 1) // size of 2nd element's identifier
    assertArrayEquals(in.readNBytes(1), "2".getBytes("UTF-8")) // identifier of 2nd element
    assertSame(tracker.get("1").get, payload(0))
    assertSame(tracker.get("2").get, payload(1))
  }

  private def givenInput(func: DataOutputStream => Unit): DataInputStream = {
    val buffer = new ByteArrayOutputStream()
    val out = new DataOutputStream(buffer)
    func(out)
    new DataInputStream(new ByteArrayInputStream(buffer.toByteArray))
  }

  private def whenOutput = givenInput _

  private def assertEndOfStream (in: DataInputStream): Unit = {
    assertEquals(-1, in.read())
  }
}
