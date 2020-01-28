# Add New Types to Serialization and Deserializaiton

This how-to provides general instructions on how to add new types to serialization and deserialization between CLR (Common Language Runtime) and JVM (Java virtual machine). The IPC (Inter-process communication) mechanism between the CLR and the JVM uses serialization to communicate. Each type has a type identifier and then an optional length followed by the actual data.

Not every type has been implemented so if you need to use a type that isn't supported and you get the error "Type {0} not supported yet", you will need to implement the type.

## Steps to follow
Please see the following E2E example of how the new type `DoubleArrayArray` is added to serialization and deserialization between CLR and JVM.

### 1. Choose a new type identifier and add it in [PayloadHelper.cs](https://github.com/dotnet/spark/blob/master/src/csharp/Microsoft.Spark/Interop/Ipc/PayloadHelper.cs) on CLR side

Example:

```csharp
private static readonly byte[] s_doubleArrayArrayTypeId = new[] { ( byte)'A' };
```

Once you have created your new identifier implement the csharp side writer in PayloadHelper.cs by finding the method `ConvertArgsToBytes` and find out where in the switch that the code should be implemented.

This is how a byte array is written:

```csharp
case double[][] argDoubleArrayArray:
    SerDe.Write(destination, s_doubleArrayArrayTypeId);
    SerDe.Write(destination, argDoubleArrayArray.Length);
    foreach (double[] doubleArray in argDoubleArrayArray)
    {
        SerDe.Write(destination, doubleArray.Length);
        foreach (double d in doubleArray)
        {
            SerDe.Write(destination, d);
        }
    }
    break;
```

Typically the type identifier is written, then the length and then the data. Once you have written the code that will perform the write you need to add a case to `GetTypeId` for your new identifier:

```csharp
if (type == typeof(int[]) ||
    type == typeof(long[]) ||
    type == typeof(double[]) ||
    type == typeof(double[][]) ||
    typeof(IEnumerable<byte[]>).IsAssignableFrom(type) ||
    typeof(IEnumerable<string>).IsAssignableFrom(type))
{
    return s_arrayTypeId;
}
```

### 2. Derserialize this new type in JVM with read method

In [SerDe.scala](https://github.com/dotnet/spark/blob/master/src/scala/microsoft-spark-2.3.x/src/main/scala/org/apache/spark/api/dotnet/SerDe.scala) (This need to be changed in all Spark version 2.3.x, 2.4.x and 3.0.x). 

In either the method `readTypedObject` or `readList` add a new case statement for your new type 
identifier:

```scala
case 'A' => readDoubleArrArr(dis)
```

Once you have read the type, you can implement your method to read the data you wrote in csharp, examples:

```scala
def readDoubleArrArr(in: DataInputStream): Array[Array[Double]] = {
  val len = readInt(in)
  (0 until len).map(_ => readDoubleArr(in)).toArray
}
```

That should be everything you need to be able to send a new type from the CLR to the JVM.

### 3. Serialize this new type in JVM with write method

In [SerDe.scala](https://github.com/dotnet/spark/blob/master/src/scala/microsoft-spark-2.3.x/src/main/scala/org/apache/spark/api/dotnet/SerDe.scala) (This need to be changed in all Spark version 2.3.x, 2.4.x and 3.0.x), the function `writeObject` contains a switch for each type:

```scala
case "[[D" =>
  writeType(dos, "list")
  writeDoubleArrArr(dos, value.asInstanceOf[Array[Array[Double]]])
```

and 

```scala
def writeDoubleArrArr(out: DataOutputStream, value: Array[Array[Double]]): Unit = {
    writeType(out, "doublearray")
    out.writeInt(value.length)
    value.foreach(v => writeDoubleArr(out, v))
}
```

`WriteType` also needs to know how to write the specific type identifier:

```scala
case "doublearray" => dos.writeByte('A'
```

### 4. Add this new type in [JvmBridge.cs](https://github.com/dotnet/spark/blob/master/src/csharp/Microsoft.Spark/Interop/Ipc/JvmBridge.cs) to read the value on CLR side

The final step is to be able to read the value on the CLR side that the JVM side has written. Back in csharp find csharp/Microsoft.Spark/Interop/Ipc/JvmBridge.cs add your type identifier to `CallJavaMethod` in the switch statement `switch (typeAsChar) //TODO: Add support for other types.` You will likely find that the type you want to implement should be implemented inside `ReadCollection`.

```csharp
case 'A':
    var doubleArrayArray = new double[numOfItemsInList][];
    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
    {
        doubleArrayArray[itemIndex] = ReadCollection(s) as double[];
    }
    returnValue = doubleArrayArray;
    break;
```
