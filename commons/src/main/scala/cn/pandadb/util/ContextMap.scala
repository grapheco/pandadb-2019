package cn.pandadb.util

import java.io.File
import scala.collection.Set
import scala.collection.mutable.{Map => MMap}

class ContextMap {
  private val _map = MMap[String, Any]();

  def keys: Set[String] = _map.keySet;

  def putAll(props: Map[String, Any]) {
    _map ++= props
  }

  def put[T](key: String, value: T): T = {
    _map(key) = value
    value
  }

  def put[T](value: T)(implicit manifest: Manifest[T]): T = put[T](manifest.runtimeClass.getName, value)

  def get[T](key: String): T = {
    _map(key).asInstanceOf[T]
  }

  def getOption[T](key: String): Option[T] = _map.get(key).map(_.asInstanceOf[T]);

  def get[T]()(implicit manifest: Manifest[T]): T = get(manifest.runtimeClass.getName);

  def getOption[T]()(implicit manifest: Manifest[T]): Option[T] = getOption(manifest.runtimeClass.getName);
}
