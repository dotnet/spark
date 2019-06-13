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
        }

        [Serializable]
        internal sealed class TargetData
        {
            public TypeData TypeData { get; set; }
            public FieldData[] Fields { get; set; }
        }

        [Serializable]
        internal sealed class FieldData
        {
            public TypeData TypeData { get; set; }
            public string Name { get; set; }
            public ValueData ValueData { get; set; }
        }

        [Serializable]
        internal sealed class ValueData : ISerializable
        {
            public ValueData() { }

            public TypeData TypeData { get; set; }

            public object Value { get; set; }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("TypeData", TypeData, typeof(TypeData));

                var bf = new BinaryFormatter();
                using (var ms = new MemoryStream())
                {
                    bf.Serialize(ms, Value);
                    info.AddValue("ValueSerialized", ms.ToArray(), typeof(byte[]));
                }
            }

            public ValueData(SerializationInfo info, StreamingContext context)
            {
                TypeData = (TypeData)info.GetValue("TypeData", typeof(TypeData));

                var valueSerialized = (byte[])info.GetValue("ValueSerialized", typeof(byte[]));
                var bf = new BinaryFormatter();
                using (var ms = new MemoryStream(valueSerialized, false))
                {
                    try
                    {
                        Value = bf.Deserialize(ms);
                    }
                    catch (SerializationException)
                    {
                        ms.Seek(0, SeekOrigin.Begin);
                        DeserializeType(TypeData);
                        Value = bf.Deserialize(ms);
                    }
                }
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
            foreach (var field in targetType.GetFields(
                BindingFlags.Instance |
                BindingFlags.Static |
                BindingFlags.Public |
                BindingFlags.NonPublic))
            {
                object value = field.GetValue(target);
                var fieldData = new FieldData()
                {
                    TypeData = SerializeType(field.FieldType),
                    Name = field.Name,
                    ValueData = new ValueData()
                    {
                        TypeData = (value != null) ? SerializeType(value.GetType()) : default,
                        Value = value
                    }
                };

                fields.Add(fieldData);
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
            var target = Activator.CreateInstance(targetType);

            foreach (FieldData field in targetData.Fields ?? Enumerable.Empty<FieldData>())
            {
                targetType.GetField(
                    field.Name,
                    BindingFlags.Instance |
                    BindingFlags.Public |
                    BindingFlags.NonPublic).SetValue(target, field.ValueData.Value);
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
            s_typeCache.GetOrAdd(typeData,
                td => LoadAssembly(typeData.ManifestModuleName).GetType(typeData.Name));

        /// <summary>
        /// Returns the loaded assembly by probing the following locations in order:
        /// 1) The working directory
        /// 2) The directory of the application
        /// If the assembly is not found in the above locations, the exception from
        /// Assembly.LoadFrom() will be propagated.
        /// </summary>
        /// <param name="manifestModuleName">The name of assembly to load</param>
        /// <returns>The loaded assembly</returns>
        private static Assembly LoadAssembly(string manifestModuleName)
        {
            var sep = Path.DirectorySeparatorChar;
            try
            {
                return Assembly.LoadFrom(
                    $"{Directory.GetCurrentDirectory()}{sep}{manifestModuleName}");
            }
            catch (FileNotFoundException)
            {
                return Assembly.LoadFrom(
                    $"{AppDomain.CurrentDomain.BaseDirectory}{sep}{manifestModuleName}");
            }
        }
    }
}
