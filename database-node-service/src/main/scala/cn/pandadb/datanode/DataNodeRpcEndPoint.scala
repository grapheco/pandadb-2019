package cn.pandadb.datanode

import java.io.File

import net.neoremind.kraps.rpc._
import org.neo4j.graphdb.{GraphDatabaseService, Label}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger
import cn.pandadb.configuration.{Config => PandaConfig}


class DataNodeRpcEndpoint(override val rpcEnv: RpcEnv, pandaConfig: PandaConfig) extends RpcEndpoint {

  val logger: Logger = pandaConfig.getLogger(this.getClass)
  val dbFile = new File(pandaConfig.getLocalNeo4jDatabasePath())
  if (!dbFile.exists()) {
    dbFile.mkdirs
  }
  var localNeo4jDB: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
  val dataNodeService = new DataNodeServiceImpl(localNeo4jDB)

  override def onStart(): Unit = {
    logger.info("start DataNodeRpcEndpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case createNodeWithId(id, labels, properties) => {
      logger.info(s"createNodeWithId($id)")
      dataNodeService.createNodeWithId(id, labels, properties)
      context.reply("createNodeWithId")
    }

    case getNode(id) => {
      logger.info(s"addNode($id)")
      dataNodeService.getNode(id)
      context.reply("getNode")
    }

  }

  override def onStop(): Unit = {
    logger.info("stop DataNodeRpcEndpoint")
  }

}
