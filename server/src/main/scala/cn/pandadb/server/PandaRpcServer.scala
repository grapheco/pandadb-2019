package cn.pandadb.datanode

import java.io.File

import cn.pandadb.blob.storage.BlobStorageService
import cn.pandadb.blob.storage.impl.LocalFileSystemBlobValueStorage
import cn.pandadb.configuration.Config
import cn.pandadb.leadernode.{LeaderNodeHandler, LeaderNodeRpcEndPoint}
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.cluster.{ClusterService, LeaderNodeChangedEvent, NodeRoleChangedEvent, NodeRoleChangedEventListener}
import cn.pandadb.index.IndexService
import cn.pandadb.server.PandaRpcHandler
import cn.pandadb.store.local.DataStore
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger

class PandaRpcServer(config: Config, clusterService: ClusterService,
                     blobStore: BlobStorageService, localDataStore: DataStore,
                     indexService: IndexService) extends LifecycleServerModule {
  val logger: Logger = config.getLogger(this.getClass)

  val dbFile = new File(localDataStore.graphStore.getAbsolutePath)
  if (!dbFile.exists()) {
    dbFile.mkdirs
  }
  var localNeo4jDB: GraphDatabaseService = new GraphDatabaseFactory()
    .newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()

  val rpcHost = config.getListenHost()
  val rpcPort = config.getRpcPort
  val rpcServerName = config.getRpcServerName()

  val rpcConfig = RpcEnvServerConfig(new RpcConf(), rpcServerName, rpcHost, rpcPort)
  val rpcEnv: HippoRpcEnv = HippoRpcEnvFactory.create(rpcConfig)

  val dataNodeEndpointName = config.getDataNodeEndpointName()
  val dataNodeRpcEndpoint: RpcEndpoint = new DataNodeRpcEndpoint(rpcEnv, config)
  var dataNodeRpcEndpointRef: RpcEndpointRef = null

  val leaderNodeEndpointName = config.getLeaderNodeEndpointName()
  val leaderNodeRpcEndpoint: RpcEndpoint = new LeaderNodeRpcEndPoint(rpcEnv, config, clusterService)
  var leaderNodeRpcEndpointRef: RpcEndpointRef = null

  //  val blobStore = new LocalFileSystemBlobValueStorage(config)

  val pandaRpcHandler = new PandaRpcHandler(config, clusterService)
  val dataNodeHandler = new DataNodeHandler(config, localNeo4jDB, blobStore, indexService, localDataStore)
  val leaderNodeHandler = new LeaderNodeHandler(config, clusterService, localNeo4jDB, blobStore, indexService, localDataStore)
  pandaRpcHandler.add(dataNodeHandler)
  pandaRpcHandler.add(leaderNodeHandler)

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
    rpcEnv.setRpcHandler(pandaRpcHandler)
    dataNodeRpcEndpointRef = rpcEnv.setupEndpoint(dataNodeEndpointName, dataNodeRpcEndpoint)
    //    clusterService.addNodeRoleChangedEventListener(new LeaderNodeChangeListener)
    addLeaderNodeRpcEndpoint()
    rpcEnv.awaitTermination()
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
    rpcEnv.stop(dataNodeRpcEndpointRef)
    this.removeLeaderNodeRpcEndpoint()
    rpcEnv.shutdown()
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  def addLeaderNodeRpcEndpoint(): Unit = {
    logger.info(this.getClass + ": addLeaderNodeRpcEndpoint")
    leaderNodeRpcEndpointRef = rpcEnv.setupEndpoint(leaderNodeEndpointName, leaderNodeRpcEndpoint)
  }

  def removeLeaderNodeRpcEndpoint(): Unit = {
    if (leaderNodeRpcEndpointRef != null) {
      logger.info(this.getClass + ": removeLeaderNodeRpcEndpoint")
      rpcEnv.stop(leaderNodeRpcEndpointRef)
    }
  }

  class LeaderNodeChangeListener extends NodeRoleChangedEventListener {
    override def notifyRoleChanged(event: NodeRoleChangedEvent): Unit = {
      logger.info(this.getClass + ": notifyRoleChanged")
      event match {
        case LeaderNodeChangedEvent(isLeader, leaderNode) =>
          if (isLeader) addLeaderNodeRpcEndpoint()
          else removeLeaderNodeRpcEndpoint()
      }
    }
  }

}
