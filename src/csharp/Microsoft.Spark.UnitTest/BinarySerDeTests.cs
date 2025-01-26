// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using System.IO;
using MessagePack;
using Microsoft.Spark.Utils;
using Xunit;

namespace Microsoft.Spark.UnitTest;

[Collection("Spark Unit Tests")]
public class BinarySerDeTests
{
    [Theory]
    [InlineData(42)]
    [InlineData("Test")]
    [InlineData(99.99)]
    public void Serialize_ShouldWriteObjectToStream(object input)
    {
        using var memoryStream = new MemoryStream();
        BinarySerDe.Serialize(memoryStream, input);
        memoryStream.Position = 0;

        var deserializedObject = MessagePackSerializer.Typeless.Deserialize(memoryStream);

        Assert.Equal(input, deserializedObject);
    }

    [Fact]
    public void Deserialize_ShouldReturnExpectedObject_WhenTypeMatches()
    {
        var employee = new Employee { Id = 101, Name = "John Doe" };
        using var memoryStream = new MemoryStream();
        MessagePackSerializer.Typeless.Serialize(memoryStream, employee);
        memoryStream.Position = 0;

        var result = BinarySerDe.Deserialize<Employee>(memoryStream);

        Assert.Equal(employee.Id, result.Id);
        Assert.Equal(employee.Name, result.Name);
    }

    [Fact]
    public void Deserialize_ShouldThrowInvalidCastEx_WhenTypeDoesNotMatch()
    {
        var employee = new Employee { Id = 101, Name = "John Doe" };
        using var memoryStream = new MemoryStream();
        MessagePackSerializer.Typeless.Serialize(memoryStream, employee);
        memoryStream.Position = 0;

        var action = () => BinarySerDe.Deserialize<Department>(memoryStream);

        Assert.Throws<InvalidCastException>(action);
    }

    [Fact]
    public void Serialize_CustomFunctionAndObject_ShouldBeSerializable()
    {
        var department = new Department { Name = "HR", EmployeeCount = 27 };
        var employeeStub = new Employee
        {
            EmbeddedObject = department,
            Id = 11,
            Name = "Derek",
        };
        using var memoryStream = new MemoryStream();
        MessagePackSerializer.Typeless.Serialize(memoryStream, employeeStub);
        memoryStream.Position = 0;

        var deserializedCalculation = BinarySerDe.Deserialize<Employee>(memoryStream);

        Assert.IsType<Department>(deserializedCalculation.EmbeddedObject);
        Assert.Equal(27, ((Department)deserializedCalculation.EmbeddedObject).EmployeeCount);
        Assert.Equal("HR", ((Department)deserializedCalculation.EmbeddedObject).Name);
    }

    [Fact]
    public void Serialize_ClassWithoutSerializableAttribute_ShouldThrowException()
    {
        var nonSerializableClass = new NonSerializableClass { Value = 123 };
        using var memoryStream = new MemoryStream();
        BinarySerDe.Serialize(memoryStream, nonSerializableClass);
        memoryStream.Position = 0;

        Assert.Throws<MessagePackSerializationException>(() => BinarySerDe.Deserialize<NonSerializableClass>(memoryStream));
    }

    [Fact]
    public void Serialize_CollectionAndDictionary_ShouldBeSerializable()
    {
        var list = new List<int> { 1, 2, 3 };
        var dictionary = new Dictionary<string, int> { { "one", 1 }, { "two", 2 } };

        using var memoryStream = new MemoryStream();
        BinarySerDe.Serialize(memoryStream, list);
        memoryStream.Position = 0;
        var deserializedList = MessagePackSerializer.Typeless.Deserialize(memoryStream) as List<int>;

        Assert.Equal(list, deserializedList);

        memoryStream.SetLength(0);
        BinarySerDe.Serialize(memoryStream, dictionary);
        memoryStream.Position = 0;
        var deserializedDictionary = MessagePackSerializer.Typeless.Deserialize(memoryStream) as Dictionary<string, int>;

        Assert.Equal(dictionary, deserializedDictionary);
    }

    [Fact]
    public void Serialize_PolymorphicObject_ShouldBeSerializable()
    {
        Employee manager = new Manager { Id = 1, Name = "Alice", Role = "Account manager" };
        using var memoryStream = new MemoryStream();
        BinarySerDe.Serialize(memoryStream, manager);
        memoryStream.Position = 0;

        var deserializedEmployee = BinarySerDe.Deserialize<Employee>(memoryStream);

        Assert.IsType<Manager>(deserializedEmployee);
        Assert.Equal("Alice", deserializedEmployee.Name);
        Assert.Equal("Account manager", ((Manager)deserializedEmployee).Role);
    }

    [Serializable]
    private class Employee
    {
        public int Id { get; set; }

        public string Name { get; set; }

        public object EmbeddedObject { get; set; }
    }

    [Serializable]
    private class Department
    {
        public string Name { get; set; }
        public int EmployeeCount { get; set; }
    }

    [Serializable]
    private class Manager : Employee
    {
        public string Role { get; set; }
    }

    private class NonSerializableClass
    {
        public int Value { get; init; }
    }
}
