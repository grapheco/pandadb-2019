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

case class DataVersionRecoveryArgs(val localLogFile: File, val clusterLogFile: File,
                                   val localNodeAddress: NodeAddress)

class LocalDataVersionRecovery(args: DataVersionRecoveryArgs) {
  val localLog = new JsonDataLogRW(args.localLogFile)
  val sinceVersion: Int = localLog.getLastVersion()
  val clusterLog = new JsonDataLogRW(args.clusterLogFile)
  val clusterVersion: Int = clusterLog.getLastVersion()

  private def _collectCypherList(): List[String] = {
    clusterLog.consume(logItem => logItem.command, sinceVersion).toList
  }

  def updateLocalVersion(): Unit = {
    if (clusterVersion > sinceVersion) {
      val cypherList = _collectCypherList()
      val boltURI = s"bolt://" + args.localNodeAddress.getAsString
      val driver = GraphDatabase.driver(boltURI)
      val session = driver.session()
      cypherList.foreach(cypher => {
        val _tx = session.beginTransaction()
        _tx.run(cypher)
        _tx.success()
        _tx.close()
      })
      session.close()
    }
  }
}