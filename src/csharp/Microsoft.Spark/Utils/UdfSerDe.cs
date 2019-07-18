// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// UdfSerDe is responsible for serializing/deserializing an UDF.
    /// </summary>
    internal class UdfSerDe
    {
        private static readonly ConcurrentDictionary<TypeData, Type> s_typeCache =
            new ConcurrentDictionary<TypeData, Type>();

        [Serializable]
        internal sealed class TypeData : IEquatable<TypeData>
        {
            public string Name { get; set; }
            public string AssemblyName { get; set; }
            public string ManifestModuleName { get; set; }

            public override int GetHashCode()
            {
                // Simply hash on the Type's Name, which should provide
                // a "unique enough" hash code.
                return Name.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return (obj is TypeData typeData) &&
                    Equals(typeData);
            }

            public bool Equals(TypeData other)
            {
                return (other != null) &&
                    (other.Name == Name) &&
                    (other.AssemblyName == AssemblyName) &&
                    (other.ManifestModuleName == ManifestModuleName);
            }
        }

        [Serializable]
        internal sealed class UdfData
        {
            public TypeData TypeData { get; set; }
            public string MethodName { get; set; }
            public TargetData TargetData { get; set; }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return (obj is UdfData udfData) &&
                    Equals(udfData);
            }

            public bool Equals(UdfData other)
            {
                return (other != null) &&
                    TypeData.Equals(other.TypeData) &&
                    (MethodName == other.MethodName) &&
                    TargetData.Equals(other.TargetData);
            }
        }

        [Serializable]
        internal sealed class TargetData
        {
            public TypeData TypeData { get; set; }
            public FieldData[] Fields { get; set; }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return (obj is TargetData targetData) &&
                    Equals(targetData);
            }

            public bool Equals(TargetData other)
            {
                if ((other == null) ||
                    !TypeData.Equals(other.TypeData) ||
                    (Fields?.Length != other.Fields?.Length))
                {
                    return false;
                }

                if ((Fields == null) && (other.Fields == null))
                {
                    return true;
                }

                return Fields.SequenceEqual(other.Fields);
            }
        }

        [Serializable]
        internal sealed class FieldData
        {
            public FieldData(object target, FieldInfo field)
            {
                object value = field.GetValue(target);

                TypeData = SerializeType(field.FieldType);
                Name = field.Name;
                ValueData = (value != null)
                    ? new ValueData(value)
                    : null;
            }

            public TypeData TypeData { get; private set; }
            public string Name { get; private set; }
            public ValueData ValueData { get; private set; }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return (obj is FieldData fieldData) &&
                    Equals(fieldData);
            }

            public bool Equals(FieldData other)
            {
                return (other != null) &&
                    TypeData.Equals(other.TypeData) &&
                    (Name == other.Name) &&
                    (((ValueData == null) && (other.ValueData == null)) ||
                        ((ValueData != null) && ValueData.Equals(other.ValueData)));
            }
        }

        /// <summary>
        /// The type of Value may be contained in an assembly outside the default
        /// load context. Upon serialization, the TypeData is preserved, and Value
        /// is serialized as a byte[]. Upon deserialization, if the assembly cannot
        /// be found within the load context then TypeData will be used to load the
        /// correct assembly.
        /// </summary>
        [Serializable]
        internal sealed class ValueData : ISerializable
        {
            public ValueData(object value)
            {
                if (value == null)
                {
                    throw new ArgumentNullException("value cannot be null.");
                }

                TypeData = SerializeType(value.GetType());
                Value = value;
            }

            public ValueData(SerializationInfo info, StreamingContext context)
            {
                TypeData = (TypeData)info.GetValue("TypeData", typeof(TypeData));
                AssemblyLoader.s_assemblyNameToFileName.TryAdd(
                    TypeData.AssemblyName,
                    TypeData.ManifestModuleName);

                var valueSerialized = (byte[])info.GetValue("ValueSerialized", typeof(byte[]));
                using (var ms = new MemoryStream(valueSerialized, false))
                {
                    var bf = new BinaryFormatter();
                    Value = bf.Deserialize(ms);
                }
            }

            public TypeData TypeData { get; private set; }

            public object Value { get; private set; }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("TypeData", TypeData, typeof(TypeData));

                using (var ms = new MemoryStream())
                {
                    var bf = new BinaryFormatter();
                    bf.Serialize(ms, Value);
                    info.AddValue("ValueSerialized", ms.ToArray(), typeof(byte[]));
                }
            }

            public override int GetHashCode()
            {
                return base.GetHashCode();
            }

            public override bool Equals(object obj)
            {
                return (obj is ValueData valueData) &&
                    Equals(valueData);
            }

            public bool Equals(ValueData other)
            {
                return (other != null) &&
                    TypeData.Equals(other.TypeData) &&
                    Value.Equals(other.Value);
            }
        }

        internal static UdfData Serialize(Delegate udf)
        {
            MethodInfo method = udf.Method;
            var target = udf.Target;

            var udfData = new UdfData()
            {
                TypeData = SerializeType(method.DeclaringType),
                MethodName = method.Name,
                TargetData = SerializeTarget(target)
            };

            return udfData;
        }

        internal static Delegate Deserialize(UdfData udfData)
        {
            Type udfType = DeserializeType(udfData.TypeData);
            MethodInfo udfMethod = udfType.GetMethod(
                udfData.MethodName,
                BindingFlags.Instance |
                BindingFlags.Static |
                BindingFlags.Public |
                BindingFlags.NonPublic);

            var udfParameters = udfMethod.GetParameters().Select(p => p.ParameterType).ToList();
            udfParameters.Add(udfMethod.ReturnType);
            Type funcType = Expression.GetFuncType(udfParameters.ToArray());

            if (udfData.TargetData == null)
            {
                // The given UDF is a static function.
                return Delegate.CreateDelegate(funcType, udfMethod);
            }
            else
            {
                return Delegate.CreateDelegate(
                    funcType,
                    DeserializeTargetData(udfData.TargetData),
                    udfData.MethodName);
            }
        }

        private static TargetData SerializeTarget(object target)
        {
            // target will be null for static functions.
            if (target == null)
            {
                return null;
            }

            Type targetType = target.GetType();
            TypeData targetTypeData = SerializeType(targetType);

            var fields = new List<FieldData>();
            foreach (FieldInfo field in targetType.GetFields(
                BindingFlags.Instance |
                BindingFlags.Static |
                BindingFlags.Public |
                BindingFlags.NonPublic))
            {
                fields.Add(new FieldData(target, field));
            }

            // Even when an UDF does not have any closure, GetFields() returns some fields
            // which include Func<> of the udf specified.
            // For now, one way to distinguish is to check if any of the field's type
            // is same as the target type. If so, fields will be emptied out.
            // TODO: Follow up with the dotnet team.
            var doesUdfHaveClosure = fields.
                Where((field) => field.TypeData.Name.Equals(targetTypeData.Name)).
                Count() == 0;

            var targetData = new TargetData()
            {
                TypeData = targetTypeData,
                Fields = doesUdfHaveClosure ? fields.ToArray() : null
            };

            return targetData;
        }

        private static object DeserializeTargetData(TargetData targetData)
        {
            Type targetType = DeserializeType(targetData.TypeData);
            var target = FormatterServices.GetUninitializedObject(targetType);

            foreach (FieldData field in targetData.Fields ?? Enumerable.Empty<FieldData>())
            {
                targetType.GetField(
                    field.Name,
                    BindingFlags.Instance |
                    BindingFlags.Public |
                    BindingFlags.NonPublic).SetValue(target, field.ValueData?.Value);
            }

            return target;
        }

        private static TypeData SerializeType(Type type)
        {
            return new TypeData()
            {
                Name = type.FullName,
                AssemblyName = type.Assembly.FullName,
                ManifestModuleName = type.Assembly.ManifestModule.Name
            };
        }

        private static Type DeserializeType(TypeData typeData) =>
            s_typeCache.GetOrAdd(
                typeData,
                td => AssemblyLoader.LoadAssembly(
                    td.AssemblyName,
                    td.ManifestModuleName).GetType(td.Name));
    }
}
