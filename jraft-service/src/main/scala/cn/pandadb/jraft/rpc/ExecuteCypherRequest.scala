package cn.pandadb.jraft.rpc

import java.util.Locale

class ExecuteCypherRequest extends Serializable {
  val serialVersionUID = -5623664785560971849L
  var cypher: String = null

  def isReadRequest(): Boolean = {
    val newCypher = cypher.toLowerCase(Locale.ROOT)
    if(newCypher.contains("create") || newCypher.contains("set") || newCypher.contains("delete")) false
    else true

  }
  def setCypher(cypher: String): Unit = {
    this.cypher = cypher
  }
  def getCypher(): String = {
    this.cypher
  }
}
