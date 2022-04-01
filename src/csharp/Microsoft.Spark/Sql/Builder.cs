// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Generic;
using Microsoft.Spark.Interop;
using Microsoft.Spark.Interop.Ipc;

namespace Microsoft.Spark.Sql
{
    /// <summary>
    /// The entry point to programming Spark with the Dataset and DataFrame API.
    /// </summary>
    public sealed class Builder : IJvmObjectReferenceProvider
    {
        private readonly Dictionary<string, string> _options =
            new Dictionary<string, string>() { { "spark.app.kind", "sparkdotnet" } };

        internal Builder()
            : this(
                (JvmObjectReference)SparkEnvironment.JvmBridge.CallStaticJavaMethod(
                    "org.apache.spark.sql.SparkSession",
                    "builder"))
        {
        }

        internal Builder(JvmObjectReference jvmObject)
        {
            Reference = jvmObject;
        }

        public JvmObjectReference Reference { get; private set; }

        /// <summary>
        /// Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
        /// run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone
        /// cluster.
        /// </summary>
        /// <param name="master">Master URL</param>
        public Builder Master(string master)
        {
            Config("spark.master", master);
            return this;
        }

        /// <summary>
        /// Sets a name for the application, which will be shown in the Spark web UI.
        /// If no application name is set, a randomly generated name will be used.
        /// </summary>
        /// <param name="appName">Name of the app</param>
        public Builder AppName(string appName)
        {
            Config("spark.app.name", appName);
            return this;
        }

        /// <summary>
        /// Sets a config option. Options set using this method are automatically propagated to
        /// both SparkConf and SparkSession's own configuration.
        /// </summary>
        /// <param name="key">Key for the configuration</param>
        /// <param name="value">value of the configuration</param>
        public Builder Config(string key, string value)
        {
            _options[key] = value;
            return this;
        }

        /// <summary>
        /// Sets a config option. Options set using this method are automatically propagated to
        /// both SparkConf and SparkSession's own configuration.
        /// </summary>
        /// <param name="key">Key for the configuration</param>
        /// <param name="value">value of the configuration</param>
        public Builder Config(string key, bool value)
        {
            _options[key] = value.ToString();
            return this;
        }

        /// <summary>
        /// Sets a config option. Options set using this method are automatically propagated to
        /// both SparkConf and SparkSession's own configuration.
        /// </summary>
        /// <param name="key">Key for the configuration</param>
        /// <param name="value">value of the configuration</param>
        public Builder Config(string key, double value)
        {
            _options[key] = value.ToString();
            return this;
        }

        /// <summary>
        /// Sets a config option. Options set using this method are automatically propagated to
        /// both SparkConf and SparkSession's own configuration.
        /// </summary>
        /// <param name="key">Key for the configuration</param>
        /// <param name="value">value of the configuration</param>
        public Builder Config(string key, long value)
        {
            _options[key] = value.ToString();
            return this;
        }

        /// <summary>
        /// Sets a list of config options based on the given SparkConf
        /// </summary>
        public Builder Config(SparkConf conf)
        {
            foreach (KeyValuePair<string, string> keyValuePair in conf.GetAll())
            {
                _options[keyValuePair.Key] = keyValuePair.Value;
            }

            return this;
        }

        /// <summary>
        /// Enables Hive support, including connectivity to a persistent Hive metastore, support
        /// for Hive serdes, and Hive user-defined functions.
        /// </summary>
        public Builder EnableHiveSupport()
        {
            return Config("spark.sql.catalogImplementation", "hive");
        }

        /// <summary>
        /// Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
        /// one based on the options set in this builder.
        /// </summary>
        /// <returns></returns>
        public SparkSession GetOrCreate()
        {
            var sparkConf = new SparkConf();
            foreach (KeyValuePair<string, string> option in _options)
            {
                sparkConf.Set(option.Key, option.Value);
            }

            Reference.Invoke("config", sparkConf);

            return new SparkSession((JvmObjectReference)Reference.Invoke("getOrCreate"));
        }
    }
}
