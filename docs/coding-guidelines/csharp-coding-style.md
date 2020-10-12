C# Coding Style
===============

We use the same [coding style](https://github.com/dotnet/runtime/blob/master/docs/coding-guidelines/coding-style.md) and [EditorConfig](https://editorconfig.org "EditorConfig homepage") file (`.editorconfig`) used by [dotnet/runtime](https://github.com/dotnet/runtime) with the following differences:

* **`var` must be used when `new`, `as`, or cast operator is used (and it can be used only with these operators).**
    ```C#
    var foo = new Foo(); // OK
    Foo foo = new Foo(); // NOT OK

    var bar = foo as Bar; // OK
    Bar bar = foo as Bar; // NOT OK
    
    var bar = (Bar)foo; // OK
    Bar bar = (Bar)foo; // NOT OK

    string str = "hello"; // OK
    var str = "hello"; // NOT OK
    int i = 0; // OK
    var i = 0; // NOT OK

    var arr = new string[] { "abc", "def" }; // OK
    string[] arr = new[] { "abc", "def" }; // NOT OK
    var arr = new[] { "abc", "def" }; // NOT OK

    string str = foo.GetString(); // Function name shouldn't matter.
    var str = foo.GetString(); // NOT OK
    ```

* **A single line statement block must go with braces.**

    ```C#
    // OK
    if (foo)
    {
        return false;
    }

    // NOT OK
    if (foo) return false;
    if (foo) { return false };

    ```
    
* **Use prefix increment/decrement operator.**
    
    Unless post increment/decrement operator usage is intended, use prefix increment/decrement operator.
    
    ```C#
    // OK
    for (int i = 0; i < arr.Length; ++i)
    
    // NOT OK
    for (int i = 0; i < arr.Length; i++)
    
    // OK
    arr[i++]; // Post increment operator usage is intended.
    ```

* **The max number of characters in a line is 110.**
    
    This can be easily done using the following line-break rules:
    
    (If you cannot find a rule for your scenario, please look through the existing code to find a match and create an issue to update this list.)
    
    * Line-break for the assignment
    ```C#
    // Try the following first to fit within the limit.
    SomeType someVariable =
        SomeMethod(arg1, arg2, arg3, arg4, arg5);
       
    // Then fall back to this.
    SomeType someVariable = SomeMethod(
        arg1,
        arg2,
        arg3,
        arg4,
        arg5);
    ```
    
    * Line-break for each method parameters:
    ```C#
    return UserDefinedFunction.Create(
        name,
        CommandSerDe.Serialize(
            execute,
            CommandSerDe.SerializedMode.Row,
            CommandSerDe.SerializedMode.Row),
        UdfUtils.GetPythonEvalType(),
        UdfUtils.GetReturnType(typeof(RT)));
    ```

    * Line-break for each method call:
    ```C#
    // If you have chained method calls, line-break each method call
    Enumerable.Range(0, numRows)
        .Select(i => i.ToString())
        .ToArray();
    ```

    There are few exceptions to this rule:

    * Log message with string interpolation:
    ```C#
    Logger.LogInfo($"This message {someVariable} is too long but try your best to fit in 100 character limit.");
    ```

    * The method signature without method parameters is long due to type paramters:
    ```C#
    public static Func<Column, Column, Column, Column, Column, Column, Column, Column, Column, Column, Column> Udf<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT>(
        Func<A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT> udf)
    ```

