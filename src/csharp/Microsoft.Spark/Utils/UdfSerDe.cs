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
            public string AssemblyFileName { get; set; }

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
                    (other.AssemblyFileName == AssemblyFileName);
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
            public TypeData TypeData { get; set; }
            public string Name { get; set; }
            public object Value { get; set; }

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
                    (((Value == null) && (other.Value == null)) ||
                        ((Value != null) && Value.Equals(other.Value)));
            }
        }

        internal static UdfData Serialize(Delegate udf)
        {
            MethodInfo method = udf.Method;
            object target = udf.Target;

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
                if (!field.GetCustomAttributes(typeof(NonSerializedAttribute)).Any())
                {
                    fields.Add(new FieldData()
                    {
                        TypeData = SerializeType(field.FieldType),
                        Name = field.Name,
                        Value = field.GetValue(target)
                    });
                }
            }

            // Even when an UDF does not have any closure, GetFields() returns some fields
            // which include Func<> of the udf specified.
            // For now, one way to distinguish is to check if any of the field's type
            // is same as the target type. If so, fields will be emptied out.
            // TODO: Follow up with the dotnet team.
            bool doesUdfHaveClosure = fields.
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
            object target = FormatterServices.GetUninitializedObject(targetType);

            foreach (FieldData field in targetData.Fields ?? Enumerable.Empty<FieldData>())
            {
                targetType.GetField(
                    field.Name,
                    BindingFlags.Instance |
                    BindingFlags.Public |
                    BindingFlags.NonPublic).SetValue(target, field.Value);
            }

            return target;
        }

        private static TypeData SerializeType(Type type)
        {
            return new TypeData()
            {
                Name = type.FullName,
                AssemblyName = type.Assembly.FullName,
                AssemblyFileName = Path.GetFileName(type.Assembly.Location)
            };
        }

        private static Type DeserializeType(TypeData typeData) =>
            s_typeCache.GetOrAdd(
                typeData,
                td =>
                {
                    Type type = AssemblyLoader.LoadAssembly(
                        td.AssemblyName,
                        td.AssemblyFileName)?.GetType(td.Name, true);
                    if (type == null)
                    {
                        throw new FileNotFoundException(
                            string.Format(
                                "Assembly '{0}' file not found '{1}'",
                                td.AssemblyName,
                                td.AssemblyFileName));
                    }

                    return type;
                });
    }
}
