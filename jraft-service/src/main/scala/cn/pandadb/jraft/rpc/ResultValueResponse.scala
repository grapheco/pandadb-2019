package cn.pandadb.jraft.rpc

import cn.pandadb.jraft.rpc.values.{NullValue, Value}
import org.neo4j.graphdb.Result

import scala.collection.immutable.Map

class ResultValueResponse(res: Result, success1: Boolean,
                          redirect1: String, errorMsg1: String) extends Serializable {

  val serialVersionUID = -4220017686727146773L
  var value: Result = res
  var success: Boolean = success1
  var redirect: String = redirect1
  var errorMsg: String = errorMsg1

  def getErrorMsg: String = this.errorMsg

  def setErrorMsg(errorMsg: String): Unit = {
    this.errorMsg = errorMsg
  }

  def getRedirect: String = this.redirect

  def setRedirect(redirect: String): Unit = {
    this.redirect = redirect
  }

  def isSuccess: Boolean = this.success

  def setSuccess(success: Boolean): Unit = {
    this.success = success
  }

  def getValue: Result = this.value

  def setValue(value: Result): Unit = {
    this.value = value
  }

  //def keys(): Iterable[String] = container.keys

 // def containsKey(key: String): Boolean = container.contains(key)

  //def get(key: String): Value = container.getOrElse(key, NullValue)

 // override def toString: String = container.toString().toString
}
