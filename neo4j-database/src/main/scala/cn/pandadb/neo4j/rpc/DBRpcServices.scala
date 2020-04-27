package cn.pandadb.neo4j.rpc

case class addNode(id: Long, labels: Array[String], properties: Map[String, String]) {
}

case class getNode(id: Long) {
}