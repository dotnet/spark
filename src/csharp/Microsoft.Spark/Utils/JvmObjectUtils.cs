// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Linq;
using System.Reflection;
using Microsoft.Spark.Interop.Ipc;
using System.Collections.Generic;

namespace Microsoft.Spark.Utils
{
    /// <summary>
    /// Provides general helper functions related to JVM objects.
    /// </summary>
    internal class JvmObjectUtils
    {
        /// <summary>
        /// Search through all assemblies in current domain and find those types
        /// that are subclasses of the parentType. For those types we find its
        /// javaClassFieldName value, which is its java-side consistent name, then
        /// construct a mapping between the java class name and dotnet class type.
        /// Please note that types containing generic parameters are not supported.
        /// </summary>
        /// <param name="parentType">The parent class of the target type.</param>
        /// <param name="javaClassFieldName">The private static string field name of the dotnet class.</param>
        /// <returns>A mapping of java class name and dotnet class type.</returns>
        internal static Dictionary<string, Type> ConstructJavaClassMapping(
            Type parentType,
            string javaClassFieldName)
        {
            // a mapping of java class name to the dotnet type
            var classMapping = new Dictionary<string, Type>();
            // search within the assemblies to find the real type that matches returnClass name
            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (Type type in assembly.GetTypes().Where(type =>
                    type.IsClass &&
                    !type.IsAbstract &&
                    type.IsSubclassOf(parentType) &&
                    !type.ContainsGenericParameters))
                {
                    FieldInfo info = type.GetField(javaClassFieldName, BindingFlags.NonPublic | BindingFlags.Static);
                    var classNameValue = (string)info.GetValue(null);
                    if (!string.IsNullOrWhiteSpace(classNameValue))
                    {
                        classMapping.Add(classNameValue, type);
                    }
                }
            }

            return classMapping;
        }

        /// <summary>
        /// Get java class name from the jvm object; search through the mapping
        /// between java class name and dotnet class type for the current domain;
        /// construct dotnet class instance by calling the constructor with jvmObject
        /// as the parameter, and cast it to type T. If the java class name doesn't
        /// exist in the mapping we assign the instance its default value.
        /// </summary>
        /// <param name="jvmObject">The reference to object created in JVM.</param>
        /// <param name="classMapping">The mapping between java class name and dotnet class type.</param>
        /// <param name="instance">The constructed dotnet object instance.</param>
        /// <typeparam name="T">The casting type of dotnet object instance.</typeparam>
        /// <returns>Whether we successfully constructed the dotnet object instance or not.</returns>
        internal static bool TryConstructInstanceFromJvmObject<T>(
            JvmObjectReference jvmObject,
            Dictionary<string, Type> classMapping,
            out T instance)
        {
            var jvmClass = (JvmObjectReference)jvmObject.Invoke("getClass");
            var returnClass = (string)jvmClass.Invoke("getTypeName");
            if (classMapping.ContainsKey(returnClass))
            {
                Type dotnetType = classMapping[returnClass];
                instance = (T)dotnetType.Assembly.CreateInstance(
                    dotnetType.FullName,
                    false,
                    BindingFlags.Instance | BindingFlags.NonPublic,
                    null,
                    new object[] { jvmObject },
                    null,
                    null);

                return true;
            }

            instance = default;
            return false;
        }
    }
}
