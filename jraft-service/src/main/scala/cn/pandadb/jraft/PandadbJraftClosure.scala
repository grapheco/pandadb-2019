package cn.pandadb.jraft

import cn.pandadb.jraft.rpc.ResultValueResponse
import cn.pandadb.jraft.rpc.values.Value
import com.alipay.sofa.jraft.{Closure, Status}
import org.neo4j.graphdb.Result

import scala.collection.immutable.Map

trait PandadbJraftClosure extends Closure{
  var valueResponse: ResultValueResponse = null
  var cypher: String = null
  def getValueResponse: ResultValueResponse = this.valueResponse
  def setValueResponse(valueResponse: ResultValueResponse): Unit = {
    this.valueResponse = valueResponse
  }

  def setCypher(cypher: String): Unit = {
    this.cypher = cypher
  }
  def getCypher: String = this.cypher
  def failure(errorMsg: String, redirect: String): Unit = {
    val response = new ResultValueResponse(null, false, null, null)
    response.setSuccess(false)
    response.setErrorMsg(errorMsg)
    response.setRedirect(redirect)
    setValueResponse(response)
  }

  def success(value: Result): Unit = {
    val response = new ResultValueResponse(null, false, null, null)
    response.setValue(value)
    response.setSuccess(true)
    setValueResponse(response)
  }

}
