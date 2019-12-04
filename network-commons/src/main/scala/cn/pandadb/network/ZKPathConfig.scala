package cn.pandadb.network

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 20:41 2019/11/26
  * @Modified By:
  */
object ZKPathConfig {
  //object
  val registryPath = s"/testPandaDB"
  val ordinaryNodesPath = registryPath + s"/ordinaryNodes"
  val leaderNodePath = registryPath + s"/leaderNode"
  val dataVersionPath = registryPath + s"/version"
  val freshNodePath = registryPath + s"/freshNode"
}
