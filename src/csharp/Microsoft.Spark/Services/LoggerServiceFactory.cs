// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Services
{
    /// <summary>
    /// Used to get logger service instances for different types
    /// </summary>
    public class LoggerServiceFactory
    {
        private static Lazy<ILoggerService> s_loggerService =
            new Lazy<ILoggerService>(() => GetDefaultLogger());

        /// <summary>
        /// Overrides an existing logger by a given logger service instance
        /// </summary>
        /// <param name="loggerServiceOverride">
        /// The logger service instance used to overrides
        /// </param>
        public static void SetLoggerService(ILoggerService loggerServiceOverride)
        {
            s_loggerService = new Lazy<ILoggerService>(() => loggerServiceOverride);
        }

        /// <summary>
        /// Gets an instance of logger service for a given type.
        /// </summary>
        /// <param name="type">The type of logger service to get</param>
        /// <returns>An instance of logger service</returns>
        public static ILoggerService GetLogger(Type type)
        {
            return s_loggerService.Value.GetLoggerInstance(type);
        }

        /// <summary>
        /// if there exists xxx.exe.config file and log4net settings, then use log4net
        /// </summary>
        /// <returns></returns>
        private static ILoggerService GetDefaultLogger()
        {
            return ConsoleLoggerService.s_instance;
        }
    }
}
