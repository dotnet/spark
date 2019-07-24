﻿// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// Runtime configuration interface for Spark.
    /// </summary>
    public sealed class RuntimeConfig : IJvmObjectReferenceProvider
    {
        private readonly JvmObjectReference _jvmObject;

        internal RuntimeConfig(JvmObjectReference jvmObject) => _jvmObject = jvmObject;

        JvmObjectReference IJvmObjectReferenceProvider.Reference => _jvmObject;

        /// <summary>
        /// Sets the given Spark runtime configuration property.
        /// </summary>
        /// <param name="key">Config name</param>
        /// <param name="value">Config value</param>
        public void Set(string key, string value) => _jvmObject.Invoke("set", key, value);

        /// <summary>
        /// Sets the given Spark runtime configuration property.
        /// </summary>
        /// <param name="key">Config name</param>
        /// <param name="value">Config value</param>
        public void Set(string key, bool value) => _jvmObject.Invoke("set", key, value);

        /// <summary>
        /// Sets the given Spark runtime configuration property.
        /// </summary>
        /// <param name="key">Config name</param>
        /// <param name="value">Config value</param>
        public void Set(string key, long value) => _jvmObject.Invoke("set", key, value);

        /// <summary>
        /// Returns the value of Spark runtime configuration property for the given key.
        /// </summary>
        /// <param name="key">Key to use</param>
        public string Get(string key) => (string)_jvmObject.Invoke("get", key);

        /// <summary>
        /// Returns the value of Spark runtime configuration property for the given key.
        /// </summary>
        /// <param name="key">Key to use</param>
        /// <param name="defaultValue">Default value to use</param>
        public string Get(string key, string defaultValue) =>
            (string)_jvmObject.Invoke("get", key, defaultValue);

        /// <summary>
        /// Resets the configuration property for the given key.
        /// </summary>
        /// <param name="key">Key to unset</param>
        public void Unset(string key) => _jvmObject.Invoke("unset", key);

        /// <summary>
        /// Indicates whether the configuration property with the given key
        /// is modifiable in the current session.
        /// </summary>
        /// <param name="key">Key to check</param>
        /// <returns>
        /// true if the configuration property is modifiable. For static SQL, Spark
        /// Core, invalid(not existing) and other non-modifiable configuration properties,
        /// the returned value is false.
        /// </returns>
        public bool IsModifiable(string key) => (bool)_jvmObject.Invoke("isModifiable", key);
    }
}
