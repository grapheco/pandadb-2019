package cn.pandadb.server

import java.io.File

import cn.pandadb.configuration.Config
import cn.pandadb.lifecycle.LifecycleSupport
import cn.pandadb.costore.CostoreServer
import cn.pandadb.cluster.ClusterService
import cn.pandadb.datanode.PandaRpcServer
import cn.pandadb.zk.ZKTools
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

class PandaServer(config: Config)  {

  val life = new LifecycleSupport
  val logger = config.getLogger(this.getClass)
  val zkTools = new ZKTools(config)
  zkTools.init()
  val localNeo4jDB = getOrCreateLocalNeo4jDatabase()
  val clusterService = new ClusterService(config, zkTools)

  life.add(clusterService)
  life.add(new CostoreServer(config) )
  life.add(new PandaRpcServer(config) )

  def start(): Unit = {
    logger.info("==== PandaDB Server Starting... ====")
    life.start()
    logger.info("==== PandaDB Server is Started ====")
  }

  def shutdown(): Unit = {
    logger.info("==== PandaDB Server Shutting Down... ====")
    life.shutdown()
    zkTools.close()
    logger.info("==== PandaDB Server is Shutdown ====")
  }

  def syncDataFromLeader(): Unit = {
    logger.info(this.getClass + ": syncDataFromLeader")
    val leaderNodeAddress = clusterService.getLeaderNode()
    val localDBPath = config.getLocalNeo4jDatabasePath()
    logger.info(s"sync data from leaderNode<$leaderNodeAddress> to local<$localDBPath>")
  }

  def getOrCreateLocalNeo4jDatabase(): GraphDatabaseService = {
    return
    val dbFile = new File(config.getLocalNeo4jDatabasePath())
    if (!dbFile.exists()) {
      dbFile.mkdirs
    }
    new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
  }
}
