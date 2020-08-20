// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using Microsoft.Spark.Network;
using Microsoft.Spark.Services;

namespace Microsoft.Spark.Interop.Ipc
{
    /// <summary>
    /// Implementation of thread safe IPC bridge between JVM and CLR
    /// Using a concurrent socket connection queue (lightweight synchronization mechanism)
    /// supporting async JVM calls like StreamingContext.AwaitTermination()
    /// </summary>
    internal sealed class JvmBridge : IJvmBridge
    {
        // TODO: On .NET Core 2.1, Span<object> could be used with a stack-based
        // two-object struct rather than having these thread-static arrays.

        [ThreadStatic]
        private static object[] s_oneArgArray;
        [ThreadStatic]
        private static object[] s_twoArgArray;
        [ThreadStatic]
        private static MemoryStream s_payloadMemoryStream;

        private readonly ConcurrentQueue<ISocketWrapper> _sockets =
            new ConcurrentQueue<ISocketWrapper>();
        private readonly ILoggerService _logger =
            LoggerServiceFactory.GetLogger(typeof(JvmBridge));
        private readonly int _portNumber;

        internal JvmBridge(int portNumber)
        {
            if (portNumber == 0)
            {
                throw new Exception("Port number is not set.");
            }

            _portNumber = portNumber;
            _logger.LogInfo($"JvMBridge port is {portNumber}");
        }

        private ISocketWrapper GetConnection()
        {
            if (!_sockets.TryDequeue(out ISocketWrapper socket))
            {
                socket = SocketFactory.CreateSocket();
                socket.Connect(IPAddress.Loopback, _portNumber);
            }

            return socket;
        }

        public JvmObjectReference CallConstructor(string className, object arg0) =>
            (JvmObjectReference)CallJavaMethod(isStatic: true, className, "<init>", arg0);

        public JvmObjectReference CallConstructor(string className, object arg0, object arg1) =>
            (JvmObjectReference)CallJavaMethod(isStatic: true, className, "<init>", arg0, arg1);

        public JvmObjectReference CallConstructor(string className, object[] args) =>
            (JvmObjectReference)CallJavaMethod(isStatic: true, className, "<init>", args);

        public object CallStaticJavaMethod(string className, string methodName, object arg0) =>
            CallJavaMethod(isStatic: true, className, methodName, arg0);

        public object CallStaticJavaMethod(
            string className,
            string methodName,
            object arg0,
            object arg1) =>
            CallJavaMethod(isStatic: true, className, methodName, arg0, arg1);

        public object CallStaticJavaMethod(string className, string methodName, object[] args) =>
            CallJavaMethod(isStatic: true, className, methodName, args);

        public object CallNonStaticJavaMethod(
            JvmObjectReference objectId,
            string methodName,
            object arg0) =>
            CallJavaMethod(isStatic: false, objectId, methodName, arg0);

        public object CallNonStaticJavaMethod(
            JvmObjectReference objectId,
            string methodName,
            object arg0,
            object arg1) =>
            CallJavaMethod(isStatic: false, objectId, methodName, arg0, arg1);

        public object CallNonStaticJavaMethod(
            JvmObjectReference objectId,
            string methodName,
            object[] args) =>
            CallJavaMethod(isStatic: false, objectId, methodName, args);

        private object CallJavaMethod(
            bool isStatic,
            object classNameOrJvmObjectReference,
            string methodName,
            object arg0)
        {
            object[] oneArgArray = s_oneArgArray ?? (s_oneArgArray = new object[1]);
            oneArgArray[0] = arg0;

            try
            {
                return CallJavaMethod(
                    isStatic,
                    classNameOrJvmObjectReference,
                    methodName,
                    oneArgArray);
            }
            finally
            {
                oneArgArray[0] = null;
            }
        }

        private object CallJavaMethod(
            bool isStatic,
            object classNameOrJvmObjectReference,
            string methodName,
            object arg0,
            object arg1)
        {
            object[] twoArgArray = s_twoArgArray ?? (s_twoArgArray = new object[2]);
            twoArgArray[0] = arg0;
            twoArgArray[1] = arg1;

            try
            {
                return CallJavaMethod(
                    isStatic,
                    classNameOrJvmObjectReference,
                    methodName,
                    twoArgArray);
            }
            finally
            {
                twoArgArray[1] = null;
                twoArgArray[0] = null;
            }
        }

        private object CallJavaMethod(
            bool isStatic,
            object classNameOrJvmObjectReference,
            string methodName,
            object[] args)
        {
            object returnValue = null;
            ISocketWrapper socket = null;
            try
            {
                MemoryStream payloadMemoryStream = s_payloadMemoryStream ??
                    (s_payloadMemoryStream = new MemoryStream());
                payloadMemoryStream.Position = 0;
                PayloadHelper.BuildPayload(
                    payloadMemoryStream,
                    isStatic,
                    classNameOrJvmObjectReference,
                    methodName,
                    args);

                socket = GetConnection();

                Stream outputStream = socket.OutputStream;
                outputStream.Write(
                    payloadMemoryStream.GetBuffer(),
                    0,
                    (int)payloadMemoryStream.Position);
                outputStream.Flush();

                Stream inputStream = socket.InputStream;
                int isMethodCallFailed = SerDe.ReadInt32(inputStream);
                if (isMethodCallFailed != 0)
                {
                    string jvmFullStackTrace = SerDe.ReadString(inputStream);
                    string errorMessage = BuildErrorMessage(
                        isStatic,
                        classNameOrJvmObjectReference,
                        methodName,
                        args);
                    _logger.LogError(errorMessage);
                    _logger.LogError(jvmFullStackTrace);
                    throw new Exception(errorMessage, new JvmException(jvmFullStackTrace));
                }

                char typeAsChar = Convert.ToChar(inputStream.ReadByte());
                switch (typeAsChar) // TODO: Add support for other types.
                {
                    case 'n':
                        break;
                    case 'j':
                        returnValue = new JvmObjectReference(SerDe.ReadString(inputStream), this);
                        break;
                    case 'c':
                        returnValue = SerDe.ReadString(inputStream);
                        break;
                    case 'i':
                        returnValue = SerDe.ReadInt32(inputStream);
                        break;
                    case 'g':
                        returnValue = SerDe.ReadInt64(inputStream);
                        break;
                    case 'd':
                        returnValue = SerDe.ReadDouble(inputStream);
                        break;
                    case 'b':
                        returnValue = Convert.ToBoolean(inputStream.ReadByte());
                        break;
                    case 'l':
                        returnValue = ReadCollection(inputStream);
                        break;
                    default:
                        // Convert typeAsChar to UInt32 because the char may be non-printable.
                        throw new NotSupportedException(
                            string.Format(
                                "Identifier for type 0x{0:X} not supported",
                                Convert.ToUInt32(typeAsChar)));
                }
                _sockets.Enqueue(socket);
            }
            catch (Exception e)
            {
                _logger.LogException(e);
                socket?.Dispose();
                throw;
            }

            return returnValue;
        }

        private string BuildErrorMessage(
            bool isStatic,
            object classNameOrJvmObjectReference,
            string methodName,
            object[] args)
        {
            var errorMessage = new StringBuilder("JVM method execution failed: ");
            const string ConstructorFormat = "Constructor failed for class '{0}'";
            const string StaticMethodFormat = "Static method '{0}' failed for class '{1}'";
            const string NonStaticMethodFormat = "Nonstatic method '{0}' failed for class '{1}'";

            try
            {
                if (isStatic)
                {
                    if (methodName.Equals("<init>")) // "<init>" is hard-coded in DotnetBackend.
                    {
                        errorMessage.AppendFormat(
                            ConstructorFormat,
                            classNameOrJvmObjectReference);
                    }
                    else
                    {
                        errorMessage.AppendFormat(
                            StaticMethodFormat,
                            methodName,
                            classNameOrJvmObjectReference);
                    }
                }
                else
                {
                    errorMessage.AppendFormat(
                        NonStaticMethodFormat,
                        methodName,
                        classNameOrJvmObjectReference);
                }

                if (args.Length == 0)
                {
                    errorMessage.Append(" when called with no arguments");
                }
                else
                {
                    errorMessage.AppendFormat(
                        " when called with {0} arguments ({1})",
                        args.Length,
                        GetArgsAsString(args));
                }
            }
            catch (Exception)
            {
                errorMessage.Append("Exception when converting building error message");
            }

            return errorMessage.ToString();
        }

        private string GetArgsAsString(IEnumerable<object> args)
        {
            var argsString = new StringBuilder();

            try
            {
                int index = 1;
                foreach (object arg in args)
                {
                    string argValue = "null";
                    string argType = "null";
                    if (arg != null)
                    {
                        argValue = arg.ToString();
                        argType = arg.GetType().Name;
                    }

                    argsString.AppendFormat(
                        "[Index={0}, Type={1}, Value={2}], ",
                        index++,
                        argType,
                        argValue);
                }
            }
            catch (Exception)
            {
                argsString.Append("Exception when converting parameters to string");
            }

            return argsString.ToString();
        }

        private object ReadCollection(Stream s)
        {
            object returnValue;
            char listItemTypeAsChar = Convert.ToChar(s.ReadByte());
            int numOfItemsInList = SerDe.ReadInt32(s);
            switch (listItemTypeAsChar)
            {
                case 'c':
                    var strArray = new string[numOfItemsInList];
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
                    {
                        strArray[itemIndex] = SerDe.ReadString(s);
                    }
                    returnValue = strArray;
                    break;
                case 'i':
                    var intArray = new int[numOfItemsInList];
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
                    {
                        intArray[itemIndex] = SerDe.ReadInt32(s);
                    }
                    returnValue = intArray;
                    break;
                case 'g':
                    var longArray = new long[numOfItemsInList];
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
                    {
                        longArray[itemIndex] = SerDe.ReadInt64(s);
                    }
                    returnValue = longArray;
                    break;
                case 'd':
                    var doubleArray = new double[numOfItemsInList];
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
                    {
                        doubleArray[itemIndex] = SerDe.ReadDouble(s);
                    }
                    returnValue = doubleArray;
                    break;
                case 'A':
                    var doubleArrayArray = new double[numOfItemsInList][];
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
                    {
                        doubleArrayArray[itemIndex] = ReadCollection(s) as double[];
                    }
                    returnValue = doubleArrayArray;
                    break;
                case 'b':
                    var boolArray = new bool[numOfItemsInList];
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
                    {
                        boolArray[itemIndex] = Convert.ToBoolean(s.ReadByte());
                    }
                    returnValue = boolArray;
                    break;
                case 'r':
                    var byteArrayArray = new byte[numOfItemsInList][];
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
                    {
                        int byteArrayLen = SerDe.ReadInt32(s);
                        byteArrayArray[itemIndex] = SerDe.ReadBytes(s, byteArrayLen);
                    }
                    returnValue = byteArrayArray;
                    break;
                case 'j':
                    var jvmObjectReferenceArray = new JvmObjectReference[numOfItemsInList];
                    for (int itemIndex = 0; itemIndex < numOfItemsInList; ++itemIndex)
                    {
                        string itemIdentifier = SerDe.ReadString(s);
                        jvmObjectReferenceArray[itemIndex] =
                            new JvmObjectReference(itemIdentifier, this);
                    }
                    returnValue = jvmObjectReferenceArray;
                    break;
                default:
                    // convert listItemTypeAsChar to UInt32 because the char may be non-printable
                    throw new NotSupportedException(
                        string.Format("Identifier for list item type 0x{0:X} not supported",
                            Convert.ToUInt32(listItemTypeAsChar)));
            }
            return returnValue;
        }

        public void Dispose()
        {
            while (_sockets.TryDequeue(out ISocketWrapper socket))
            {
                if (socket != null)
                {
                    socket.Dispose();
                }
            }
        }
    }
}
