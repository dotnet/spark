// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Spark.Sql;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Helper to build the IPC payload for JVM calls from CLR.
    /// </summary>
    internal class PayloadHelper
    {
        private static readonly byte[] s_int32TypeId = new[] { (byte)'i' };
        private static readonly byte[] s_int64TypeId = new[] { (byte)'g' };
        private static readonly byte[] s_stringTypeId = new[] { (byte)'c' };
        private static readonly byte[] s_boolTypeId = new[] { (byte)'b' };
        private static readonly byte[] s_doubleTypeId = new[] { (byte)'d' };
        private static readonly byte[] s_jvmObjectTypeId = new[] { (byte)'j' };
        private static readonly byte[] s_byteArrayTypeId = new[] { (byte)'r' };
        private static readonly byte[] s_intArrayTypeId = new[] { (byte)'l' };
        private static readonly byte[] s_dictionaryTypeId = new[] { (byte)'e' };
        private static readonly byte[] s_rowArrTypeId = new[] { (byte)'R' };

        private static readonly ConcurrentDictionary<Type, bool> s_isDictionaryTable =
            new ConcurrentDictionary<Type, bool>();

        internal static void BuildPayload(
            MemoryStream destination,
            bool isStaticMethod,
            object classNameOrJvmObjectReference,
            string methodName,
            object[] args)
        {
            // Reserve space for total length.
            var originalPosition = destination.Position;
            destination.Position += sizeof(int);

            SerDe.Write(destination, isStaticMethod);
            SerDe.Write(destination, classNameOrJvmObjectReference.ToString());
            SerDe.Write(destination, methodName);
            SerDe.Write(destination, args.Length);
            ConvertArgsToBytes(destination, args);

            // Write the length now that we've written out everything else.
            var afterPosition = destination.Position;
            destination.Position = originalPosition;
            SerDe.Write(destination, (int)afterPosition - sizeof(int));
            destination.Position = afterPosition;
        }

        internal static void ConvertArgsToBytes(
            MemoryStream destination,
            object[] args,
            bool addTypeIdPrefix = true)
        {
            long posBeforeEnumerable, posAfterEnumerable;
            int itemCount;
            object[] convertArgs = null;

            foreach (object arg in args)
            {
                if (arg == null)
                {
                    destination.WriteByte((byte)'n');
                    continue;
                }

                Type argType = arg.GetType();

                if (addTypeIdPrefix)
                {
                    SerDe.Write(destination, GetTypeId(argType));
                }

                switch (Type.GetTypeCode(argType))
                {
                    case TypeCode.Int32:
                        SerDe.Write(destination, (int)arg);
                        break;

                    case TypeCode.Int64:
                        SerDe.Write(destination, (long)arg);
                        break;

                    case TypeCode.String:
                        SerDe.Write(destination, (string)arg);
                        break;

                    case TypeCode.Boolean:
                        SerDe.Write(destination, (bool)arg);
                        break;

                    case TypeCode.Double:
                        SerDe.Write(destination, (double)arg);
                        break;

                    case TypeCode.Object:
                        switch (arg)
                        {
                            case byte[] argByteArray:
                                SerDe.Write(destination, argByteArray.Length);
                                SerDe.Write(destination, argByteArray);
                                break;

                            case int[] argInt32Array:
                                SerDe.Write(destination, s_int32TypeId);
                                SerDe.Write(destination, argInt32Array.Length);
                                foreach (int i in argInt32Array)
                                {
                                    SerDe.Write(destination, i);
                                }
                                break;

                            case long[] argInt64Array:
                                SerDe.Write(destination, s_int64TypeId);
                                SerDe.Write(destination, argInt64Array.Length);
                                foreach (long i in argInt64Array)
                                {
                                    SerDe.Write(destination, i);
                                }
                                break;

                            case double[] argDoubleArray:
                                SerDe.Write(destination, s_doubleTypeId);
                                SerDe.Write(destination, argDoubleArray.Length);
                                foreach (double d in argDoubleArray)
                                {
                                    SerDe.Write(destination, d);
                                }
                                break;

                            case IEnumerable<byte[]> argByteArrayEnumerable:
                                SerDe.Write(destination, s_byteArrayTypeId);
                                posBeforeEnumerable = destination.Position;
                                destination.Position += sizeof(int);
                                itemCount = 0;
                                foreach (byte[] b in argByteArrayEnumerable)
                                {
                                    ++itemCount;
                                    SerDe.Write(destination, b.Length);
                                    destination.Write(b, 0, b.Length);
                                }
                                posAfterEnumerable = destination.Position;
                                destination.Position = posBeforeEnumerable;
                                SerDe.Write(destination, itemCount);
                                destination.Position = posAfterEnumerable;
                                break;

                            case IEnumerable<string> argStringEnumerable:
                                SerDe.Write(destination, s_stringTypeId);
                                posBeforeEnumerable = destination.Position;
                                destination.Position += sizeof(int);
                                itemCount = 0;
                                foreach (string s in argStringEnumerable)
                                {
                                    ++itemCount;
                                    SerDe.Write(destination, s);
                                }
                                posAfterEnumerable = destination.Position;
                                destination.Position = posBeforeEnumerable;
                                SerDe.Write(destination, itemCount);
                                destination.Position = posAfterEnumerable;
                                break;

                            case IEnumerable<IJvmObjectReferenceProvider> argJvmEnumerable:
                                SerDe.Write(destination, s_jvmObjectTypeId);
                                posBeforeEnumerable = destination.Position;
                                destination.Position += sizeof(int);
                                itemCount = 0;
                                foreach (IJvmObjectReferenceProvider jvmObject in argJvmEnumerable)
                                {
                                    ++itemCount;
                                    SerDe.Write(destination, jvmObject.Reference.Id);
                                }
                                posAfterEnumerable = destination.Position;
                                destination.Position = posBeforeEnumerable;
                                SerDe.Write(destination, itemCount);
                                destination.Position = posAfterEnumerable;
                                break;

                            case var _ when IsDictionary(arg.GetType()):
                                // Generic dictionary, but we don't have it strongly typed as
                                // Dictionary<T,U>
                                var dictInterface = (IDictionary)arg;
                                var dict = new Dictionary<object, object>(dictInterface.Count);
                                IDictionaryEnumerator iter = dictInterface.GetEnumerator();
                                while (iter.MoveNext())
                                {
                                    dict[iter.Key] = iter.Value;
                                }

                                // Below serialization is corresponding to deserialization method
                                // ReadMap() of SerDe.scala.

                                // dictionary's length
                                SerDe.Write(destination, dict.Count);

                                // keys' data type
                                SerDe.Write(
                                    destination,
                                    GetTypeId(arg.GetType().GetGenericArguments()[0]));
                                // keys' length, same as dictionary's length
                                SerDe.Write(destination, dict.Count);
                                if (convertArgs == null)
                                {
                                    convertArgs = new object[1];
                                }
                                foreach (KeyValuePair<object, object> kv in dict)
                                {
                                    convertArgs[0] = kv.Key;
                                    // keys, do not need type prefix.
                                    ConvertArgsToBytes(destination, convertArgs, false);
                                }

                                // values' length, same as dictionary's length
                                SerDe.Write(destination, dict.Count);
                                foreach (KeyValuePair<object, object> kv in dict)
                                {
                                    convertArgs[0] = kv.Value;
                                    // values, need type prefix.
                                    ConvertArgsToBytes(destination, convertArgs, true);
                                }
                                break;

                            case IJvmObjectReferenceProvider argProvider:
                                SerDe.Write(destination, argProvider.Reference.Id);
                                break;

                            case IEnumerable<GenericRow> argRowArray:
                                SerDe.Write(destination, (int)argRowArray.Count());
                                foreach (GenericRow r in argRowArray)
                                {
                                    SerDe.Write(destination, (int)r.Values.Length);
                                    ConvertArgsToBytes(destination, r.Values, true);                                    
                                }
                                break;

                            default:
                                throw new NotSupportedException(
                                    string.Format($"Type {arg.GetType()} is not supported"));
                        }
                        break;
                }
            }
        }

        internal static byte[] GetTypeId(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Int32:
                    return s_int32TypeId;
                case TypeCode.Int64:
                    return s_int64TypeId;
                case TypeCode.String:
                    return s_stringTypeId;
                case TypeCode.Boolean:
                    return s_boolTypeId;
                case TypeCode.Double:
                    return s_doubleTypeId;
                case TypeCode.Object:
                    if (typeof(IJvmObjectReferenceProvider).IsAssignableFrom(type))
                    {
                        return s_jvmObjectTypeId;
                    }

                    if (type == typeof(byte[]))
                    {
                        return s_byteArrayTypeId;
                    }

                    if (type == typeof(int[]) ||
                        type == typeof(long[]) ||
                        type == typeof(double[]) ||
                        typeof(IEnumerable<byte[]>).IsAssignableFrom(type) ||
                        typeof(IEnumerable<string>).IsAssignableFrom(type))
                    {
                        return s_intArrayTypeId;
                    }

                    if (IsDictionary(type))
                    {
                        return s_dictionaryTypeId;
                    }

                    if (typeof(IEnumerable<IJvmObjectReferenceProvider>).IsAssignableFrom(type))
                    {
                        return s_intArrayTypeId;
                    }

                    if (type == typeof(IEnumerable<GenericRow>))                        
                    {
                        return s_rowArrTypeId;
                    }
                    break;
            }

            // TODO: Support other types.
            throw new NotSupportedException(string.Format("Type {0} not supported yet", type));
        }

        private static bool IsDictionary(Type type)
        {
            if (!s_isDictionaryTable.TryGetValue(type, out var isDictionary))
            {
                s_isDictionaryTable[type] = isDictionary =
                    type.GetInterfaces().Any(
                        i => i.IsGenericType &&
                            (i.GetGenericTypeDefinition() == typeof(IDictionary<,>)));
            }
            return isDictionary;
        }
    }
}
