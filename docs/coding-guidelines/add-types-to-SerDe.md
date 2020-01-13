The IPC mechanism between the CLR and the JVM uses serialization to communicate. Each type has a
 type identifier and then an optional length followed by the actual data.

Not every type has been implemented so if you need to use a type that isn't supported and you get 
the error "Type {0} not supported yet", you will need to implement the type.

Steps you need to take:

1. Choose a new type identifier and add it to:

https://github.com/dotnet/spark/blob/master/src/csharp/Microsoft.Spark/Interop/Ipc/PayloadHelper.cs#L20

Example:

```csharp

    private static readonly byte[] s_int32TypeId = new[] { (byte)'i' };
    private static readonly byte[] s_int64TypeId = new[] { (byte)'g' };
    private static readonly byte[] s_stringTypeId = new[] { (byte)'c' };
    private static readonly byte[] s_boolTypeId = new[] { (byte)'b' };
    private static readonly byte[] s_doubleTypeId = new[] { (byte)'d' };
    private static readonly byte[] s_jvmObjectTypeId = new[] { (byte)'j' };
    private static readonly byte[] s_byteArrayTypeId = new[] { (byte)'r' };
    private static readonly byte[] s_arrayTypeId = new[] { (byte)'l' };
    private static readonly byte[] s_dictionaryTypeId = new[] { (byte)'e' };
    private static readonly byte[] s_rowArrTypeId = new[] { (byte)'R' };

```

Once you have created your new identifier implement the csharp side writer in PayloadHelper.cs by 
finding the method `ConvertArgsToBytes` and find out where in the switch that the code should be 
implemented.

This is how a byte array is written:

```csharp

    if (addTypeIdPrefix)
    {
        SerDe.Write(destination, GetTypeId(argType));
    }

...

    case byte[] argByteArray:
        SerDe.Write(destination, argByteArray.Length);
        SerDe.Write(destination, argByteArray);
        break;

```

Typically the type identifier is written, then the length and then the data. Once you have written 
the code that will perform the write you need to add a case to `GetTypeId` for your new identifier:

```csharp

    case TypeCode.Double:
        return s_doubleTypeId;

```

The next step is to add the derserialization code to scala (read code).

In src/main/scala/org/apache/spark/api/dotnet/SerDe.scala (These are versioned so you may need to 
try and find it). In the method `readTypedObject` add a new case statement for your new type 
identifier:

```scala

def readTypedObject(dis: DataInputStream, dataType: Char): Object = {
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
      case 'j' => JVMObjectTracker.getObject(readString(dis))
      case 'R' => readRowArr(dis)
      case _ => throw new IllegalArgumentException(s"Invalid type $dataType")
    }
  }

```

Once you have read the type, you can implement your method to read the data you wrote in csharp, examples:

```scala

  def readBoolean(in: DataInputStream): Boolean = {
    in.readBoolean()
  }

  def readRow(in: DataInputStream): Row = {
    val len = readInt(in)
    Row.fromSeq((0 until len).map(_ => readObject(in)))
  }

  def readBytesArr(in: DataInputStream): Array[Array[Byte]] = {
    val len = readInt(in)
    (0 until len).map(_ => readBytes(in)).toArray
  }

```

That should be everything you need to be able to send a new type from the CLR to the JVM but we now
 need to write back from the JVM to the CLR.

In src/main/scala/org/apache/spark/api/dotnet/SerDe.scala the function `writeObject` contains a 
switch for each type, a couple of examples:

```scala

    case "int" | "java.lang.Integer" =>
        writeType(dos, "integer")
        writeInt(dos, value.asInstanceOf[Int])

    case "java.sql.Timestamp" =>
          writeType(dos, "time")
          writeTime(dos, value.asInstanceOf[Timestamp])

    case "[D" =>
          writeType(dos, "list")
          writeDoubleArr(dos, value.asInstanceOf[Array[Double]])

```

```scala

 def writeInt(out: DataOutputStream, value: Int): Unit = {
    out.writeInt(value)
  }

 def writeTime(out: DataOutputStream, value: Time): Unit = {
    out.writeDouble(value.getTime.toDouble / 1000.0)
  }

 def writeDoubleArr(out: DataOutputStream, value: Array[Double]): Unit = {
    writeType(out, "double")
    out.writeInt(value.length)
    value.foreach(v => out.writeDouble(v))
  }

```

The final step is to be able to read the value on the CLR side that the JVM side has written. Back 
in csharp find csharp/Microsoft.Spark/Interop/Ipc/JvmBridge.cs add your type identifier to 
`CallJavaMethod` in the switch statement `switch (typeAsChar) //TODO: Add support for other types.`
you will likely find that the type you want to implement should be implemented inside 
`ReadCollection`.
