package cn.pandadb.util

import scala.collection.Set
import scala.collection.mutable.{Map => MMap}

class ContextMap {
  private val _map = MMap[String, Any]();

  def keys: Set[String] = _map.keySet;

  protected def put[T](key: String, value: T): T = {
    _map(key) = value
    value
  }

  protected def put[T](value: T)(implicit manifest: Manifest[T]): T = put[T](manifest.runtimeClass.getName, value)

  protected def get[T](key: String): T = {
    _map(key).asInstanceOf[T]
  }

  protected def getOption[T](key: String): Option[T] = _map.get(key).map(_.asInstanceOf[T]);

  protected def get[T]()(implicit manifest: Manifest[T]): T = get(manifest.runtimeClass.getName);

  protected def getOption[T]()(implicit manifest: Manifest[T]): Option[T] = getOption(manifest.runtimeClass.getName);

  def toConfiguration: Configuration = new Configuration() {
    override def getRaw(name: String): Option[String] = getOption(name)
  }
}

object GlobalContext extends ContextMap {
  def setLeaderNode(f: Boolean): Unit = super.put("isLeaderNode", f)

  def setWatchDog(f: Boolean): Unit = super.put("isWatchDog", f)

  def isWatchDog(): Boolean = super.getOption[Boolean]("isWatchDog").getOrElse(false)

  def isLeaderNode(): Boolean = super.getOption[Boolean]("isLeaderNode").getOrElse(false)
}