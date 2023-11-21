package com.verizon.oneparser.broker.consumergroup.utils

import org.apache.spark.internal.Logging

import scala.util.control.NonFatal

object ReflectionHelper extends Logging {

  import scala.reflect.runtime.universe.{TermName, TypeTag, runtimeMirror, typeOf}

  private val currentMirror = runtimeMirror(getClass.getClassLoader)

  def reflectField[T, OUT](obj: Any, fieldName: String)(implicit ttag: TypeTag[T]): Option[OUT] = {
    val relMirror = currentMirror.reflect(obj)

    try {
      val method = typeOf[T].decl(TermName(fieldName)).asTerm.accessed.asTerm

      Some(relMirror.reflectField(method).get.asInstanceOf[OUT])
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to reflect field $fieldName from $obj. $e")
        None
    }
  }

  def reflectFieldWithContextClassloaderLoosenType(obj: Any, fieldName: String): Option[Any] = {
    val typeMirror = runtimeMirror(Thread.currentThread().getContextClassLoader)
    val instanceMirror = typeMirror.reflect(obj)

    val members = instanceMirror.symbol.typeSignature.members
    val field = members.find(_.name.decodedName.toString == fieldName)
    field match {
      case Some(f) =>
        try {
          Some(instanceMirror.reflectField(f.asTerm).get)
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to reflect field $fieldName from $obj. $e")
            None
        }

      case None =>
        logWarning(s"Failed to reflect field $fieldName from $obj.")
        None
    }
  }

  def reflectFieldWithContextClassloader[OUT](obj: Any, fieldName: String): Option[OUT] = {
    reflectFieldWithContextClassloaderLoosenType(obj, fieldName).map(_.asInstanceOf[OUT])
  }

  def reflectMethodWithContextClassloaderLoosenType(obj: Any, methodName: String, params: Any*): Option[Any] = {
    val typeMirror = runtimeMirror(Thread.currentThread().getContextClassLoader)
    val instanceMirror = typeMirror.reflect(obj)

    val members = instanceMirror.symbol.typeSignature.members
    val method = members.find(_.name.decodedName.toString == methodName)
    method match {
      case Some(f) =>
        try {
          Some(instanceMirror.reflectMethod(f.asMethod).apply(params))
        } catch {
          case NonFatal(_) =>
            logWarning(s"Failed to call method $methodName from $obj via reflection.")
            None
        }

      case None =>
        logWarning(s"Failed to call method $methodName from $obj via reflection.")
        None
    }
  }

  def reflectMethodWithContextClassloader[OUT](obj: Any, fieldName: String, params: Any*): Option[OUT] =
    reflectMethodWithContextClassloaderLoosenType(obj, fieldName, params: _*).map(_.asInstanceOf[OUT])

  def classForName(className: String): Class[_] = Class.forName(className, true, getContextOrClassClassLoader)

  private def getContextOrClassClassLoader: ClassLoader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
}
