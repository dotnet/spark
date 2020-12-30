/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import org.apache.spark.sql.Row
import org.junit.Assert._
import org.junit.{Before, Test}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.sql.Date
import scala.collection.JavaConverters._

@Test
class SerDeTest {
  private var sut: SerDe = _
  private var tracker: JVMObjectTracker = _

  @Before
  def before(): Unit = {
    tracker = new JVMObjectTracker
    sut = new SerDe(tracker)
  }

  @Test
  def shouldReadNull(): Unit = {
    val in = withStreamState(out => {
      out.writeByte('n')
    })

    assertEquals(null, sut.readObject(in))
  }

  @Test
  def shouldThrowForUnsupportedTypes(): Unit = {
    val in = withStreamState(out => {
      out.writeByte('_')
    })

    assertThrows(classOf[IllegalArgumentException], () => {
      sut.readObject(in)
    })
  }

  @Test
  def shouldReadInteger(): Unit = {
    val in = withStreamState(out => {
      out.writeByte('i')
      out.writeInt(42)
    })

    assertEquals(42, sut.readObject(in))
  }

  @Test
  def shouldReadLong(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('g')
      state.writeLong(42)
    })

    assertEquals(42L, sut.readObject(in))
  }

  @Test
  def shouldReadDouble(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('d')
      state.writeDouble(42.42)
    })

    assertEquals(42.42, sut.readObject(in))
  }

  @Test
  def shouldReadBoolean(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('b')
      state.writeBoolean(true)
    })

    assertEquals(true, sut.readObject(in))
  }

  @Test
  def shouldReadString(): Unit = {
    val payload = "Spark Dotnet"
    val in = withStreamState(state => {
      state.writeByte('c')
      state.writeInt(payload.getBytes("UTF-8").length)
      state.write(payload.getBytes("UTF-8"))
    })

    assertEquals(payload, sut.readObject(in))
  }

  @Test
  def shouldReadMap(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('e') // map type descriptor
      state.writeInt(3) // size
      state.writeByte('i') // key type
      state.writeInt(3) // number of keys
      state.writeInt(11) // first key
      state.writeInt(22) // second key
      state.writeInt(33) // third key
      state.writeInt(3) // number of values
      state.writeByte('b') // first value type
      state.writeBoolean(true) // first value
      state.writeByte('d') // second value type
      state.writeDouble(42.42) // second value
      state.writeByte('n') // third type & value
    })

    assertEquals(
      mapAsJavaMap(Map(
        11 -> true,
        22 -> 42.42,
        33 -> null)),
      sut.readObject(in))
  }

  @Test
  def shouldReadEmptyMap(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('e') // map type descriptor
      state.writeInt(0) // size
    })

    assertEquals(mapAsJavaMap(Map()), sut.readObject(in))
  }

  @Test
  def shouldReadBytesArray(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('r') // byte array type descriptor
      state.writeInt(3) // length
      state.write(Array[Byte](1, 2, 3)) // payload
    })

    assertArrayEquals(Array[Byte](1, 2, 3), sut.readObject(in).asInstanceOf[Array[Byte]])
  }

  @Test
  def shouldReadEmptyBytesArray(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('r') // byte array type descriptor
      state.writeInt(0) // length
    })

    assertArrayEquals(Array[Byte](), sut.readObject(in).asInstanceOf[Array[Byte]])
  }

  @Test
  def shouldReadEmptyList(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('l') // type descriptor
      state.writeByte('i') // element type
      state.writeInt(0) // length
    })

    assertArrayEquals(Array[Int](), sut.readObject(in).asInstanceOf[Array[Int]])
  }

  @Test
  def shouldReadList(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('l') // type descriptor
      state.writeByte('b') // element type
      state.writeInt(3) // length
      state.writeBoolean(true)
      state.writeBoolean(false)
      state.writeBoolean(true)
    })

    assertArrayEquals(Array(true, false, true), sut.readObject(in).asInstanceOf[Array[Boolean]])
  }

  @Test
  def shouldThrowWhenReadingListWithUnsupportedType(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('l') // type descriptor
      state.writeByte('_') // unsupported element type
    })

    assertThrows(classOf[IllegalArgumentException], () => {
      sut.readObject(in)
    })
  }

  @Test
  def shouldReadDate(): Unit = {
    val in = withStreamState(state => {
      val date = "2020-12-31"
      state.writeByte('D') // type descriptor
      state.writeInt(date.getBytes("UTF-8").length) // date string size
      state.write(date.getBytes("UTF-8"))
    })

    assertEquals(Date.valueOf("2020-12-31"), sut.readObject(in))
  }

  @Test
  def shouldReadObject(): Unit = {
    val trackingObject = new Object
    tracker.put(trackingObject)
    val in = withStreamState(state => {
      val objectIndex = "1"
      state.writeByte('j') // type descriptor
      state.writeInt(objectIndex.getBytes("UTF-8").length) // size
      state.write(objectIndex.getBytes("UTF-8"))
    })

    assertSame(trackingObject, sut.readObject(in))
  }

  @Test
  def shouldThrowWhenReadingNonTrackingObject(): Unit = {
    val in = withStreamState(state => {
      val objectIndex = "42"
      state.writeByte('j') // type descriptor
      state.writeInt(objectIndex.getBytes("UTF-8").length) // size
      state.write(objectIndex.getBytes("UTF-8"))
    })

    assertThrows(classOf[NoSuchElementException], () => {
      sut.readObject(in)
    })
  }

  @Test
  def shouldReadSparkRows(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('R') // type descriptor
      state.writeInt(2) // number of rows
      state.writeInt(1) // number of elements in 1st row
      state.writeByte('i') // type of 1st element in 1st row
      state.writeInt(11)
      state.writeInt(3) // number of elements in 2st row
      state.writeByte('b') // type of 1st element in 2nd row
      state.writeBoolean(true)
      state.writeByte('d') // type of 2nd element in 2nd row
      state.writeDouble(42.24)
      state.writeByte('g') // type of 3nd element in 2nd row
      state.writeLong(99)
    })

    assertEquals(
      seqAsJavaList(Seq(
        Row.fromSeq(Seq(11)),
        Row.fromSeq(Seq(true, 42.24, 99)))),
      sut.readObject(in))
  }

  @Test
  def shouldReadArrayOfObjects(): Unit = {
    val in = withStreamState(state => {
      state.writeByte('O') // type descriptor
      state.writeInt(2) // number of elements
      state.writeByte('i') // type of 1st element
      state.writeInt(42)
      state.writeByte('b') // type of 2nd element
      state.writeBoolean(true)
    })

    assertEquals(Seq[Any](42, true), sut.readObject(in).asInstanceOf[Seq[Any]])
  }

  private def withStreamState(func: DataOutputStream => Unit): DataInputStream = {
    val buffer = new ByteArrayOutputStream();
    val out = new DataOutputStream(buffer)
    func(out)
    new DataInputStream(new ByteArrayInputStream(buffer.toByteArray))
  }
}
