# Guide to using Broadcast Variables

This is a guide to show how to use Broadcast variables in .NET for Apache Spark.

## What are Broadcast variables

[Broadcast variables in Apache Spark](https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#broadcast-variables) is a mechanism for sharing variables across executors that are meant to be read-only. They allow the programmer to keep a read-only variable cached on each machine rather than shipping a copy of it with tasks. They can be used, for example, to give every node a copy of a large input dataset in an efficient manner.

### How to use Broadcast variables in .NET for Apache Spark

Broadcast variables are created from a variable `v` by calling `SparkContext.Broadcast(v)`. The broadcast variable is a wrapper around `v`, and it's value can be accessed by calling the `Value()` method on it. 

Example:

```csharp
string v = "Variable to be broadcasted";
Broadcast<string> bv = SparkContext.Broadcast(v);

// Using the broadcast variable in a UDF:
Func<Column, Column> udf = Udf<string, string>(
	str => $"{str}: {bv.Value()}");
```

The type of broadcast variable is captured by using Generics in C# as can be seen in the above example.

### Deleting broadcast variables

The broadcast variable can be deleted from all executors by calling the `Destroy()` function on it.

```csharp
// Destroying the broadcast variable bv:
bv.Destroy();
```

> Note: `Destroy` deletes all data and metadata related to the broadcast variable. Use this with caution; once a broadcast variable has been destroyed, it cannot be used again.

#### Caveat of using Destroy

One important thing to keep in mind while using Broadcast variables in UDFs is to limit the scope of the variable to only the UDF that is referencing it. The [guide to using UDFs](udf-guide.md) describes this phenomenon in detail. This is especially crucial when calling `Destroy` on the broadcast variable, as if the broadcast variable that has been destroyed is visible to/accessible from other UDFs, it gets picked up for serialization by all those UDFs even if it is not being referenced by them; and throws an error as .NET for Apache Spark is not able to serialize the destroyed broadcast variable.

Example to demonstrate:

```csharp
string v = "Variable to be broadcasted";
Broadcast<string> bv = SparkContext.Broadcast(v);

// Using the broadcast variable in a UDF:
Func<Column, Column> udf1 = Udf<string, string>(
	str => $"{str}: {bv.Value()}");

// Destroying bv
bv.Destroy();

// Calling udf1 after destroying bv throws the following exception:
// org.apache.spark.SparkException: Attempted to use Broadcast(0) after it was destroyed
df.Select(udf1(df["_1"])).Show();

// Different UDF udf2 that is not referencing bv, throws error
Func<Column, Column> udf2 = Udf<string, string>(
	str => $"{str}: not referencing broadcast variable");

// Calling udf2 that is not referencing bv throws the following exception:
// [Error] [JvmBridge] org.apache.spark.SparkException: Task not serializable

df.Select(udf2(df["_1"])).Show();
```

The recommended way of implementing above desired behavior:

```csharp
string v = "Variable to be broadcasted";
// Restricting the visibility of bv to only the UDF referencing it
{
	Broadcast<string> bv = SparkContext.Broadcast(v);

	// Using the broadcast variable in a UDF:
	Func<Column, Column> udf1 = Udf<string, string>(
		str => $"{str}: {bv.Value()}");

	// Destroying bv
	bv.Destroy();
}

// Different UDF udf2 that is not referencing bv
Func<Column, Column> udf2 = Udf<string, string>(
	str => $"{str}: not referencing broadcast variable");

df.Select(udf2(df["_1"])).Show();
```
