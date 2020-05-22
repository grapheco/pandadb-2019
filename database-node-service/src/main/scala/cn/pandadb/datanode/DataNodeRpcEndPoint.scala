package cn.pandadb.datanode

import java.io.File
import java.nio.ByteBuffer

import net.neoremind.kraps.rpc._
import org.neo4j.graphdb.{GraphDatabaseService}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger
import cn.pandadb.configuration.{Config => PandaConfig}
import cn.pandadb.util.{PandaReplyMessage}


class DataNodeRpcEndpoint(override val rpcEnv: RpcEnv, pandaConfig: PandaConfig) extends RpcEndpoint {

  val logger: Logger = pandaConfig.getLogger(this.getClass)
  //  val dbFile = new File(pandaConfig.getLocalNeo4jDatabasePath())
  //  if (!dbFile.exists()) {
  //    dbFile.mkdirs
  //  }
  //  var localNeo4jDB: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
  //  val dataNodeService = new DataNodeServiceImpl(localNeo4jDB)

  override def onStart(): Unit = {
    logger.info("start DataNodeRpcEndpoint")
  }

  override def onStop(): Unit = {
    logger.info("stop DataNodeRpcEndpoint")
  }

}
