// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;

namespace Microsoft.Spark.Services
{
    /// <summary>
    /// Defines a logger what be used in service
    /// </summary>
    internal interface ILoggerService
    {
        /// <summary>
        /// Gets a value indicating whether logging is enabled for the Debug level.
        /// </summary>
        bool IsDebugEnabled { get; }

        /// <summary>
        /// Get an instance of ILoggerService by a given type of logger
        /// </summary>
        /// <param name="type">The type of a logger to return</param>
        /// <returns>An instance of ILoggerService</returns>
        ILoggerService GetLoggerInstance(Type type);

        /// <summary>
        /// Logs a message at debug level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        void LogDebug(string message);

        /// <summary>
        /// Logs a message at debug level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        void LogDebug(string messageFormat, params object[] messageParameters);

        /// <summary>
        /// Logs a message at info level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        void LogInfo(string message);

        /// <summary>
        /// Logs a message at info level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        void LogInfo(string messageFormat, params object[] messageParameters);

        /// <summary>
        /// Logs a message at warning level.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        void LogWarn(string message);

        /// <summary>
        /// Logs a message at warning level with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        void LogWarn(string messageFormat, params object[] messageParameters);

        /// <summary>
        /// Logs a fatal message.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        void LogFatal(string message);

        /// <summary>
        /// Logs a fatal message with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        void LogFatal(string messageFormat, params object[] messageParameters);

        /// <summary>
        /// Logs a error message.
        /// </summary>
        /// <param name="message">The message to be logged</param>
        void LogError(string message);

        /// <summary>
        /// Logs a error message with a format string.
        /// </summary>
        /// <param name="messageFormat">The format string</param>
        /// <param name="messageParameters">The array of arguments</param>
        void LogError(string messageFormat, params object[] messageParameters);

        /// <summary>
        /// Logs an exception
        /// </summary>
        /// <param name="e">The exception to be logged</param>
        void LogException(Exception e);
    }
}
