package cn.pandadb.driver

import cn.pandadb.network.{ClusterClient, NodeAddress}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}

/**
 * @Author: codeBabyLin
 * @Description:
 * @Date: Created at 20:50 2019/11/27
 * @Modified By:
 */

object SelectNode {


  private def getWriteNode(clusterOperator: ClusterClient): NodeAddress = {
    //isReadNode = false
    //clusterOperator.getWriteMasterNode()
    val hos = "10.0.86.179"
    val por = 7687
    new NodeAddress(hos, por)
  }
  private def getReadNode(clusterOperator: ClusterClient): NodeAddress = {
    //random to pick up a node

    //isReadNode = true
    //val nodeLists = clusterOperator.getAllNodes().toList
    //val index = (new util.Random).nextInt(nodeLists.length)
    // nodeLists(index)
    val hos = "10.0.86.179"
    val por = 7687
    new NodeAddress(hos, por)
  }
  private def getNode(isWriteStatement: Boolean, clusterOperator: ClusterClient): NodeAddress = {
    if (isWriteStatement) getWriteNode(clusterOperator) else getReadNode(clusterOperator)
  }

  def getDriver(isWriteStatement: Boolean, clusterOperator: ClusterClient): Driver = {
    val node = getNode(isWriteStatement, clusterOperator)
    val host = node.host
    val port = node.port
    val uri = s"bolt://$host:$port"
    GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "123456"))
  }
}
