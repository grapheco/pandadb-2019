package cn.pandadb.server

import java.io.File

import cn.pandadb.network.NodeAddress
import org.neo4j.driver.GraphDatabase

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 23:06 2019/12/2
  * @Modified By:
  */

/*
duties:
1. read log
2. get current version number
3. get to do list  cypherList: List[String]
4. run cypher strings in the sorted list.
5. register itself to zk after recovery
 */

/*
how to write? use driver?
what if this node is selected as leader node?
 */

/*
add global version in the MasterRole or ZKClusterClient?
 */
case class DataVersionRecoveryArgs(val localLogFile: File, val clusterLogFile: File,
                                   val localNodeAddress: NodeAddress)


class LocalDataVersionRecovery(args: DataVersionRecoveryArgs) {
  val localLog = new JsonDataLog(args.localLogFile)
  val sinceVersion: Int = localLog.getLastVersion()
  val clusterLog = new JsonDataLog(args.clusterLogFile)
  val clusterVersion: Int = clusterLog.getLastVersion()

  private def _collectCypherList(): List[String] = {
    clusterLog.consume(logItem => logItem.command, sinceVersion).toList
  }

  def updateLocalVersion(): Unit = {
    if (clusterVersion > sinceVersion) {
      val cypherList = _collectCypherList()
      val boltURI = s"bolt://" + args.localNodeAddress.getAsStr()
      val driver = GraphDatabase.driver(boltURI)
      val session = driver.session()
      cypherList.foreach(cypher => {
        val _tx = session.beginTransaction()
        _tx.run(cypher)
        _tx.success()
        _tx.close()
      })
    }
  }
}