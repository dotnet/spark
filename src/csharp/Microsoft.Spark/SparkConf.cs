// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark
{
    /// <summary>
    /// Configuration for a Spark application. Used to set various Spark parameters as key-value
    /// pairs.
    /// </summary>
    /// <remarks>
    /// Note that once a SparkConf object is passed to Spark, it is cloned and can no longer be
    /// modified by the user. Spark does not support modifying the configuration at runtime.
    /// </remarks>
    public sealed class SparkConf : IJvmObjectReferenceProvider
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="loadDefaults">
        /// Indicates whether to load values from Java system properties
        /// </param>
        public SparkConf(bool loadDefaults = true)
            : this(SparkEnvironment.JvmBridge.CallConstructor(
                "org.apache.spark.SparkConf",
                loadDefaults))
        {
        }

        internal SparkConf(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;

            // Special handling for debug mode because spark.master and spark.app.name will not
            // be set in debug mode. Driver code may override these values if SetMaster or
            // SetAppName methods are used.
            if (string.IsNullOrWhiteSpace(Get("spark.master", "")))
            {
                SetMaster("local");
            }
            if (string.IsNullOrWhiteSpace(Get("spark.app.name", "")))
            {
                SetAppName("debug app");
            }
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// The master URL to connect to, such as "local" to run locally with one thread,
        /// "local[4]" to run locally with 4 cores, or "spark://master:7077" to run on a Spark
        /// standalone cluster.
        /// </summary>
        /// <param name="master">Spark master</param>
        public SparkConf SetMaster(string master)
        {
            Reference.Invoke("setMaster", master);
            return this;
        }

        /// <summary>
        /// Set a name for your application. Shown in the Spark web UI.
        /// </summary>
        /// <param name="appName">Name of the app</param>
        public SparkConf SetAppName(string appName)
        {
            Reference.Invoke("setAppName", appName);
            return this;
        }

        /// <summary>
        /// Set the location where Spark is installed on worker nodes.
        /// </summary>
        /// <param name="sparkHome"></param>
        /// <returns></returns>
        public SparkConf SetSparkHome(string sparkHome)
        {
            Reference.Invoke("setSparkHome", sparkHome);
            return this;
        }

        /// <summary>
        /// Set the value of a string config
        /// </summary>
        /// <param name="key">Config name</param>
        /// <param name="value">Config value</param>
        public SparkConf Set(string key, string value)
        {
            Reference.Invoke("set", key, value);
            return this;
        }

        /// <summary>
        /// Get a int parameter value, falling back to a default if not set
        /// </summary>
        /// <param name="key">Key to use</param>
        /// <param name="defaultValue">Default value to use</param>
        public int GetInt(string key, int defaultValue)
        {
            return (int)Reference.Invoke("getInt", key, defaultValue);
        }

        /// <summary>
        /// Get a string parameter value, falling back to a default if not set
        /// </summary>
        /// <param name="key">Key to use</param>
        /// <param name="defaultValue">Default value to use</param>
        public string Get(string key, string defaultValue)
        {
            return (string)Reference.Invoke("get", key, defaultValue);
        }

        /// <summary>
        /// Get all parameters as a list of pairs.
        /// </summary>
        public IReadOnlyList<KeyValuePair<string, string>> GetAll()
        {
            var kvpStringCollection = (string)Reference.Jvm.CallStaticJavaMethod(
                "org.apache.spark.sql.api.dotnet.JvmBridgeUtils",
                "getSparkConfAsString",
                this);

            string[] kvpStringArray = kvpStringCollection.Split(';');
            var configs = new List<KeyValuePair<string, string>>();

            foreach (string kvpString in kvpStringArray)
            {
                if (!string.IsNullOrEmpty(kvpString))
                {
                    string[] kvpItems = kvpString.Split('=');
                    if ((kvpItems.Length == 2) &&
                        !string.IsNullOrEmpty(kvpItems[0]) &&
                        !string.IsNullOrEmpty(kvpItems[1]))
                    {
                        configs.Add(new KeyValuePair<string, string>(kvpItems[0], kvpItems[1]));
                    }
                }
            }

            return configs;
        }
    }
}
