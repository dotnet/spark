// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
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

        private const int SocketBufferThreshold = 3;
        private const int ThreadIdForRepl = 1;

        private readonly int _processId = Process.GetCurrentProcess().Id;
        private readonly SemaphoreSlim _socketSemaphore;
        private readonly ConcurrentQueue<ISocketWrapper> _sockets =
            new ConcurrentQueue<ISocketWrapper>();
        private readonly ILoggerService _logger =
            LoggerServiceFactory.GetLogger(typeof(JvmBridge));
        private readonly int _portNumber;
        private readonly JvmThreadPoolGC _jvmThreadPoolGC;
        private readonly bool _isRunningRepl;

        internal JvmBridge(int portNumber)
        {
            if (portNumber == 0)
            {
                throw new Exception("Port number is not set.");
            }

            _portNumber = portNumber;
            _logger.LogInfo($"JvMBridge port is {portNumber}");

            _jvmThreadPoolGC = new JvmThreadPoolGC(
                _logger, this, SparkEnvironment.ConfigurationService.JvmThreadGCInterval, _processId);

            _isRunningRepl = SparkEnvironment.ConfigurationService.IsRunningRepl();

            int numBackendThreads = SparkEnvironment.ConfigurationService.GetNumBackendThreads();
            int maxNumSockets = numBackendThreads;
            if (numBackendThreads >= (2 * SocketBufferThreshold))
            {
                // Set the max number of concurrent sockets to be less than the number of
                // JVM backend threads to allow some buffer.
                maxNumSockets -= SocketBufferThreshold;
            }
            _logger.LogInfo($"The number of JVM backend thread is set to {numBackendThreads}. " +
                $"The max number of concurrent sockets in JvmBridge is set to {maxNumSockets}.");
            _socketSemaphore = new SemaphoreSlim(maxNumSockets, maxNumSockets);
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
            JvmObjectReference jvmObject,
            string methodName,
            object arg0) =>
            CallJavaMethod(isStatic: false, jvmObject, methodName, arg0);

        public object CallNonStaticJavaMethod(
            JvmObjectReference jvmObject,
            string methodName,
            object arg0,
            object arg1) =>
            CallJavaMethod(isStatic: false, jvmObject, methodName, arg0, arg1);

        public object CallNonStaticJavaMethod(
            JvmObjectReference jvmObject,
            string methodName,
            object[] args) =>
            CallJavaMethod(isStatic: false, jvmObject, methodName, args);

        private object CallJavaMethod(
            bool isStatic,
            object classNameOrJvmObjectReference,
            string methodName,
            object arg0)
        {
            object[] oneArgArray = s_oneArgArray ??= new object[1];
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
            object[] twoArgArray = s_twoArgArray ??= new object[2];
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
                // Limit the number of connections to the JVM backend. Netty is configured
                // to use a set number of threads to process incoming connections. Each
                // new connection is delegated to these threads in a round robin fashion.
                // A deadlock can occur on the JVM if a new connection is scheduled on a
                // blocked thread.
                _socketSemaphore.Wait();

                // dotnet-interactive does not have a dedicated thread to process
                // code submissions and each code submission can be processed in different
                // threads. DotnetHandler uses the CLR thread id to ensure that the same
                // JVM thread is used to handle the request, which means that code submitted
                // through dotnet-interactive may be executed in different JVM threads. To
                // mitigate this, when running in the REPL, submit requests to the DotnetHandler
                // using the same thread id. This mitigation has some limitations in multithreaded
                // scenarios. If a JVM method is blocking and needs a JVM method call issued by a
                // separate thread to unblock it, then this scenario is not supported.
                //
                // ie, `StreamingQuery.AwaitTermination()` is a blocking call and requires
                // `StreamingQuery.Stop()` to be called to unblock it. However, the `Stop`
                // call will never run because DotnetHandler will assign the method call to
                // run on the same thread that `AwaitTermination` is running on.
                Thread thread = _isRunningRepl ? null : Thread.CurrentThread;
                int threadId = thread == null ? ThreadIdForRepl : thread.ManagedThreadId;
                MemoryStream payloadMemoryStream = s_payloadMemoryStream ??= new MemoryStream();
                payloadMemoryStream.Position = 0;

                PayloadHelper.BuildPayload(
                    payloadMemoryStream,
                    isStatic,
                    _processId,
                    threadId,
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

                if (thread != null)
                {
                    _jvmThreadPoolGC.TryAddThread(thread);
                }

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

                if (e.InnerException is JvmException)
                {
                    // DotnetBackendHandler caught JVM exception and passed back to dotnet.
                    // We can reuse this connection.
                    if (socket != null) // Safety check
                    {
                        _sockets.Enqueue(socket);
                    }
                }
                else
                {
                    if (e.InnerException is SocketException)
                    {
                        _logger.LogError(
                            "Scala worker abandoned the connection, likely fatal crash on Java side. \n" +
                            "Ensure Spark runs with sufficient memory.");
                    }

                    // In rare cases we may hit the Netty connection thread deadlock.
                    // If max backend threads is 10 and we are currently using 10 active
                    // connections (0 in the _sockets queue). When we hit this exception,
                    // the socket?.Dispose() will not requeue this socket and we will release
                    // the semaphore. Then in the next thread (assuming the other 9 connections
                    // are still busy), a new connection will be made to the backend and this
                    // connection may be scheduled on the blocked Netty thread.
                    socket?.Dispose();
                }

                throw;
            }
            finally
            {
                _socketSemaphore.Release();
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
            _jvmThreadPoolGC.Dispose();
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
