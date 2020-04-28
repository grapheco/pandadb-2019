package cn.pandadb.datanode

import java.io.File

import cn.pandadb.configuration.Config
import cn.pandadb.neo4j.rpc.{DBRpcEndpoint, DBRpcServer}
import cn.pandadb.server.modules.LifecycleServerModule
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.neo4j.graphdb.{GraphDatabaseService, Label}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger


class DataNodeRpcServer(config: Config) extends LifecycleServerModule {
  val logger: Logger = config.getLogger(this.getClass)
  val rpcHost = config.getListenHost()
  val rpcPort = config.getRpcPort
  val rpcServerName = config.getRpcServerName()
  val rpcEndpointName = config.getRpcEndpointName()
  val dbFile = new File(config.getLocalNeo4jDatabasePath())
  if (!dbFile.exists()) {
    dbFile.mkdirs
  }
  var localNeo4jDB: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()
  val dataNodeService = new DataNodeServiceImpl(localNeo4jDB)
  // dynamic set by cluster service
  setNodeRole(true)

  def setNodeRole(isLeader: Boolean): Unit = {
    if (isLeader) dataNodeService.setLeader()
    else dataNodeService.cancelLeader()
  }

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
    val rpcConfig = RpcEnvServerConfig(new RpcConf(), rpcServerName, rpcHost, rpcPort)
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(rpcConfig)
    val dbEndpoint: RpcEndpoint = new DataNodeRpcEndpoint(rpcEnv, logger, dataNodeService)
    rpcEnv.setupEndpoint(rpcEndpointName, dbEndpoint)
    rpcEnv.awaitTermination()
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

}
