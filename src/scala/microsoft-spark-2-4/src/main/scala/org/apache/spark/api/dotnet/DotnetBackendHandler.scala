/*
 * Licensed to the .NET Foundation under one or more agreements.
 * The .NET Foundation licenses this file to you under the MIT license.
 * See the LICENSE file in the project root for more information.
 */

package org.apache.spark.api.dotnet

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import scala.language.existentials

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Handler for DotnetBackend.
 * This implementation is similar to RBackendHandler.
 */
class DotnetBackendHandler(server: DotnetBackend, objectsTracker: JVMObjectTracker)
    extends SimpleChannelInboundHandler[Array[Byte]]
    with Logging {

  private[this] val serDe = new SerDe(objectsTracker)

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val reply = handleBackendRequest(msg)
    ctx.write(reply)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  def handleBackendRequest(msg: Array[Byte]): Array[Byte] = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    // First bit is isStatic
    val isStatic = serDe.readBoolean(dis)
    val processId = serDe.readInt(dis)
    val threadId = serDe.readInt(dis)
    val objId = serDe.readString(dis)
    val methodName = serDe.readString(dis)
    val numArgs = serDe.readInt(dis)

    if (objId == "DotnetHandler") {
      methodName match {
        case "stopBackend" =>
          serDe.writeInt(dos, 0)
          serDe.writeType(dos, "void")
          server.close()
        case "rm" =>
          try {
            val t = serDe.readObjectType(dis)
            assert(t == 'c')
            val objToRemove = serDe.readString(dis)
            objectsTracker.remove(objToRemove)
            serDe.writeInt(dos, 0)
            serDe.writeObject(dos, null)
          } catch {
            case e: Exception =>
              logError(s"Removing $objId failed", e)
              serDe.writeInt(dos, -1)
          }
        case "rmThread" =>
          try {
            assert(serDe.readObjectType(dis) == 'i')
            val processId = serDe.readInt(dis)
            assert(serDe.readObjectType(dis) == 'i')
            val threadToDelete = serDe.readInt(dis)
            val result = ThreadPool.tryDeleteThread(processId, threadToDelete)
            serDe.writeInt(dos, 0)
            serDe.writeObject(dos, result.asInstanceOf[AnyRef])
          } catch {
            case e: Exception =>
              logError(s"Removing thread $threadId failed", e)
              serDe.writeInt(dos, -1)
          }
        case "connectCallback" =>
          assert(serDe.readObjectType(dis) == 'c')
          val address = serDe.readString(dis)
          assert(serDe.readObjectType(dis) == 'i')
          val port = serDe.readInt(dis)
          server.setCallbackClient(address, port)
          serDe.writeInt(dos, 0)

          // Sends reference of CallbackClient to dotnet side,
          // so that dotnet process can send the client back to Java side
          // when calling any API containing callback functions.
          serDe.writeObject(dos, server.callbackClient)
        case "closeCallback" =>
          logInfo("Requesting to close callback client")
          server.shutdownCallbackClient()
          serDe.writeInt(dos, 0)
          serDe.writeType(dos, "void")

        case _ => dos.writeInt(-1)
      }
    } else {
      ThreadPool
        .run(processId, threadId, () => handleMethodCall(isStatic, objId, methodName, numArgs, dis, dos))
    }

    bos.toByteArray
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // Skip logging the exception message if the connection was disconnected from
    // the .NET side so that .NET side doesn't have to explicitly close the connection via
    // "stopBackend." Note that an exception is still thrown if the exit status is non-zero,
    // so skipping this kind of exception message does not affect the debugging.
    if (!cause.getMessage.contains(
          "An existing connection was forcibly closed by the remote host")) {
      logError("Exception caught: ", cause)
    }

    // Close the connection when an exception is raised.
    ctx.close()
  }

  def handleMethodCall(
      isStatic: Boolean,
      objId: String,
      methodName: String,
      numArgs: Int,
      dis: DataInputStream,
      dos: DataOutputStream): Unit = {
    var obj: Object = null
    var args: Array[java.lang.Object] = null
    var methods: Array[java.lang.reflect.Method] = null

    try {
      val cls = if (isStatic) {
        Utils.classForName(objId)
      } else {
        objectsTracker.get(objId) match {
          case None => throw new IllegalArgumentException("Object not found " + objId)
          case Some(o) =>
            obj = o
            o.getClass
        }
      }

      args = readArgs(numArgs, dis)
      methods = cls.getMethods

      val selectedMethods = methods.filter(m => m.getName == methodName)
      if (selectedMethods.length > 0) {
        val index = findMatchedSignature(selectedMethods.map(_.getParameterTypes), args)

        if (index.isEmpty) {
          logWarning(
            s"cannot find matching method ${cls}.$methodName. "
              + s"Candidates are:")
          selectedMethods.foreach { method =>
            logWarning(s"$methodName(${method.getParameterTypes.mkString(",")})")
          }
          throw new Exception(s"No matched method found for $cls.$methodName")
        }

        val ret = selectedMethods(index.get).invoke(obj, args: _*)

        // Write status bit
        serDe.writeInt(dos, 0)
        serDe.writeObject(dos, ret.asInstanceOf[AnyRef])
      } else if (methodName == "<init>") {
        // methodName should be "<init>" for constructor
        val ctor = cls.getConstructors.filter { x =>
          matchMethod(numArgs, args, x.getParameterTypes)
        }.head

        val obj = ctor.newInstance(args: _*)

        serDe.writeInt(dos, 0)
        serDe.writeObject(dos, obj.asInstanceOf[AnyRef])
      } else {
        throw new IllegalArgumentException(
          "invalid method " + methodName + " for object " + objId)
      }
    } catch {
      case e: Throwable =>
        val jvmObj = objectsTracker.get(objId)
        val jvmObjName = jvmObj match {
          case Some(jObj) => jObj.getClass.getName
          case None => "NullObject"
        }
        val argsStr = args
          .map(arg => {
            if (arg != null) {
              s"[Type=${arg.getClass.getCanonicalName}, Value: $arg]"
            } else {
              "[Value: NULL]"
            }
          })
          .mkString(", ")

        logError(s"Failed to execute '$methodName' on '$jvmObjName' with args=($argsStr)")

        if (methods != null) {
          logDebug(s"All methods for $jvmObjName:")
          methods.foreach(m => logDebug(m.toString))
        }

        serDe.writeInt(dos, -1)
        serDe.writeString(dos, Utils.exceptionString(e.getCause))
    }
  }

  // Read a number of arguments from the data input stream
  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] = {
    (0 until numArgs).map { arg =>
      serDe.readObject(dis)
    }.toArray
  }

  // Checks if the arguments passed in args matches the parameter types.
  // NOTE: Currently we do exact match. We may add type conversions later.
  def matchMethod(
      numArgs: Int,
      args: Array[java.lang.Object],
      parameterTypes: Array[Class[_]]): Boolean = {
    if (parameterTypes.length != numArgs) {
      return false
    }

    for (i <- 0 until numArgs) {
      val parameterType = parameterTypes(i)
      var parameterWrapperType = parameterType

      // Convert native parameters to Object types as args is Array[Object] here
      if (parameterType.isPrimitive) {
        parameterWrapperType = parameterType match {
          case java.lang.Integer.TYPE => classOf[java.lang.Integer]
          case java.lang.Long.TYPE => classOf[java.lang.Long]
          case java.lang.Double.TYPE => classOf[java.lang.Double]
          case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
          case _ => parameterType
        }
      }

      if (!parameterWrapperType.isInstance(args(i))) {
        // non primitive types
        if (!parameterType.isPrimitive && args(i) != null) {
          return false
        }

        // primitive types
        if (parameterType.isPrimitive && !parameterWrapperType.isInstance(args(i))) {
          return false
        }
      }
    }

    true
  }

  // Find a matching method signature in an array of signatures of constructors
  // or methods of the same name according to the passed arguments. Arguments
  // may be converted in order to match a signature.
  //
  // Note that in Java reflection, constructors and normal methods are of different
  // classes, and share no parent class that provides methods for reflection uses.
  // There is no unified way to handle them in this function. So an array of signatures
  // is passed in instead of an array of candidate constructors or methods.
  //
  // Returns an Option[Int] which is the index of the matched signature in the array.
  def findMatchedSignature(
      parameterTypesOfMethods: Array[Array[Class[_]]],
      args: Array[Object]): Option[Int] = {
    val numArgs = args.length

    for (index <- parameterTypesOfMethods.indices) {
      val parameterTypes = parameterTypesOfMethods(index)

      if (parameterTypes.length == numArgs) {
        var argMatched = true
        var i = 0
        while (i < numArgs && argMatched) {
          val parameterType = parameterTypes(i)

          if (parameterType == classOf[Seq[Any]] && args(i).getClass.isArray) {
            // The case that the parameter type is a Scala Seq and the argument
            // is a Java array is considered matching. The array will be converted
            // to a Seq later if this method is matched.
          } else {
            var parameterWrapperType = parameterType

            // Convert native parameters to Object types as args is Array[Object] here
            if (parameterType.isPrimitive) {
              parameterWrapperType = parameterType match {
                case java.lang.Integer.TYPE => classOf[java.lang.Integer]
                case java.lang.Long.TYPE => classOf[java.lang.Long]
                case java.lang.Double.TYPE => classOf[java.lang.Double]
                case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
                case _ => parameterType
              }
            }
            if ((parameterType.isPrimitive || args(i) != null) &&
                !parameterWrapperType.isInstance(args(i))) {
              argMatched = false
            }
          }

          i = i + 1
        }

        if (argMatched) {
          // For now, we return the first matching method.
          // TODO: find best method in matching methods.

          // Convert args if needed
          val parameterTypes = parameterTypesOfMethods(index)

          for (i <- 0 until numArgs) {
            if (parameterTypes(i) == classOf[Seq[Any]] && args(i).getClass.isArray) {
              // Convert a Java array to scala Seq
              args(i) = args(i).asInstanceOf[Array[_]].toSeq
            }
          }

          return Some(index)
        }
      }
    }
    None
  }

  def logError(id: String, e: Exception): Unit = {}
}


