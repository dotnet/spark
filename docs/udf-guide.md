# Guide to User-Defined Functions (UDF)

This is a guide to show how to use UDFs in .NET for Apache Spark.

## What are UDFs

[User-Defined Functions (UDFs)](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/expressions/UserDefinedFunction.html) are a feature of Spark that allow developers to use custom functions to extend the vocabulary of Spark SQL’s Domain Specific Language. They transform values from a single row within a table to produce a single corresponding output value per row based on the logic defined in the UDF.

Let's take the following as an example for a UDF definition:

```csharp
string s1 = "hello";
Func<Column, Column> udf = Udf<string, string>(
    str => $"{s1} {str}");

```

And for a sample Dataframe, let's take the following Dataframe `df`:

```text
    +-------+
    |   name|
    +-------+
    |Michael|
    |   Andy|
    | Justin|
    +-------+
```

Now let's apply the above defined `udf` to the dataframe `df`:

```csharp
DataFrame udfResult = df.Select(udf(df["name"]));
```

This would return the below as the Dataframe `udfResult`:

```text
    +-------------+
    |         name|
    +-------------+
    |hello Michael|
    |   hello Andy|
    | hello Justin|
    +-------------+
```

## Good to know while implementing UDFs

One behavior to be aware of while implementing UDFs in .NET for Apache Spark is the way target of the UDF gets serialized. .NET for Apache Spark uses .NET Core which does not support serializing delegates, so it is instead done by using reflection to serialize the target where the delegate is defined. When multiple delegates are defined in a common scope, they have a shared closure that becomes the target of reflection for serialization. Let's take an example to illustrate what that means:

Sample user code:

```csharp
using System;

public class C {
    public void M() {
        string s1 = "s1";
        string s2 = "s2";
        Func<string, string> a = str => s1;
        Func<string, string> b = str => s2;
    }
}
```

The above C# code generates the following IL (Intermediate language) code from the compiler:

```csharp
public class C
{
    [CompilerGenerated]
    private sealed class <>c__DisplayClass0_0
    {
        public string s1;

        public string s2;

        internal string <M>b__0(string str)
        {
            return s1;
        }

        internal string <M>b__1(string str)
        {
            return s2;
        }
    }

    public void M()
    {
        <>c__DisplayClass0_0 <>c__DisplayClass0_ = new <>c__DisplayClass0_0();
        <>c__DisplayClass0_.s1 = "s1";
        <>c__DisplayClass0_.s2 = "s2";
        Func<string, string> func = new Func<string, string>(<>c__DisplayClass0_.<M>b__0);
        Func<string, string> func2 = new Func<string, string>(<>c__DisplayClass0_.<M>b__1);
    }
}
```
As can be seen in the above IL code, both `func` and `func2` share the same closure `<>c__DisplayClass0_0`, which is the target that is serialized when serializing the delegates `func` and `func2`. Hence, even though `Func<string, string> a` is only referencing `s1`, `s2` also gets serialized when sending over the bytes to the workers.
This can lead to some unexpected behaviors at runtime (for example in the case of using Broadcast variables, as explained in more detail in [this guide](broadcast-guide.md)), which is why we recommend restricting the visibility of the variables used in a function to that function's scope.
Taking the above example to better explain what that means:

Recommended user code:

```csharp
using System;

public class C {
    public void M() {
        string s1 = "s1";
        string s2 = "s2";
        Func<string, string> a = str => s1;
        Func<string, string> b = str => s2;
    }
}
```

The above C# code generates the following IL (Intermediate language) code from the compiler:

```csharp
public class C
{
    [CompilerGenerated]
    private sealed class <>c__DisplayClass0_0
    {
        public string s1;

        internal string <M>b__0(string str)
        {
            return s1;
        }
    }

    [CompilerGenerated]
    private sealed class <>c__DisplayClass0_1
    {
        public string s2;

        internal string <M>b__1(string str)
        {
            return s2;
        }
    }

    public void M()
    {
        <>c__DisplayClass0_0 <>c__DisplayClass0_ = new <>c__DisplayClass0_0();
        <>c__DisplayClass0_.s1 = "s1";
        Func<string, string> func = new Func<string, string>(<>c__DisplayClass0_.<M>b__0);
        <>c__DisplayClass0_1 <>c__DisplayClass0_2 = new <>c__DisplayClass0_1();
        <>c__DisplayClass0_2.s2 = "s2";
        Func<string, string> func2 = new Func<string, string>(<>c__DisplayClass0_2.<M>b__1);
    }
}
```

Here we see that `func` and `func2` no longer share a closure and have their own separate closures `<>c__DisplayClass0_0` and `<>c__DisplayClass0_1` respectively, which when used as target for serialization mean that no more than the referenced variables get serialized for the delegate.

