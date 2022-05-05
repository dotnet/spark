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
    /// JvmObjectUtils is used to hold basic general helper functions that
    /// are used within ML scope.
    /// </summary>
    internal class JvmObjectUtils
    {
        /// <summary>
        /// Helper function for constructing the mapping between java class name and dotnet class type.
        /// </summary>
        /// <param name="parentType">The parent class of the target type.</param>
        /// <param name="javaClassFieldName">The private static string field name of the dotnet class.</param>
        /// <returns>a mapping of className and dotnet class type</returns>
        internal static Dictionary<string, Type> ConstructJavaClassMapping(
            Type parentType,
            string javaClassFieldName)
        {
            // a mapping of java class name to the dotnet type
            var classMapping = new Dictionary<string, Type>();
            // search within the assemblies to find the real type that matches returnClass name
            foreach (Assembly assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                foreach (Type type in assembly.GetTypes().Where(
                    type => type.IsClass && !type.IsAbstract && type.IsSubclassOf(parentType)))
                {
                    FieldInfo info = type.GetField(javaClassFieldName, BindingFlags.NonPublic | BindingFlags.Static);
                    var classNameValue = type.ContainsGenericParameters ? null : (string)info.GetValue(null);
                    if (classNameValue != null) classMapping.Add(classNameValue, type);
                }
            }
            return classMapping;
        }

        /// <summary>
        /// Helper function for reconstructing the exact dotnet object from jvm object.
        /// </summary>
        /// <param name="jvmObject">The reference to object created in JVM.</param>
        /// <param name="classMapping">The mapping between java class name and dotnet class type.</param>
        /// <returns>the object instance</returns>
        internal static object ConstructInstanceFromJvmObject(
            JvmObjectReference jvmObject,
            Dictionary<string, Type> classMapping)
        {
            var jvmClass = (JvmObjectReference)jvmObject.Invoke("getClass");
            var returnClass = (string)jvmClass.Invoke("getTypeName");
            Type constructorClass = null;
            object instance = null;
            if (classMapping.ContainsKey(returnClass))
            {
                constructorClass = classMapping[returnClass];
                instance = constructorClass.Assembly.CreateInstance(
                        constructorClass.FullName, false,
                        BindingFlags.Instance | BindingFlags.NonPublic,
                        null, new object[] { jvmObject }, null, null);
            }
            return instance;
        }
    }
}