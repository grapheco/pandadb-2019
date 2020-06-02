package cn.pandadb.datanode

import cn.pandadb.blob.storage.BlobStorageService
import cn.pandadb.blob.storage.impl.LocalFileSystemBlobValueStorage
import cn.pandadb.configuration.Config
import cn.pandadb.leadernode.LeaderNodeRpcEndPoint
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.cluster.{ClusterService, LeaderNodeChangedEvent, NodeRoleChangedEvent, NodeRoleChangedEventListener}
import cn.pandadb.server.{PandaRpcHandler}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.{HippoRpcEnv, HippoRpcEnvFactory}
import org.slf4j.Logger


class PandaRpcServer(config: Config, clusterService: ClusterService) extends LifecycleServerModule {
  val logger: Logger = config.getLogger(this.getClass)

  val rpcHost = config.getListenHost()
  val rpcPort = config.getRpcPort
  val rpcServerName = config.getRpcServerName()

  val rpcConfig = RpcEnvServerConfig(new RpcConf(), rpcServerName, rpcHost, rpcPort)
  val rpcEnv: HippoRpcEnv = HippoRpcEnvFactory.create(rpcConfig)

  val blobStore = new LocalFileSystemBlobValueStorage(config)
  val pandaRpcHandler = new PandaRpcHandler(config, clusterService, blobStore)

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
    rpcEnv.setRpcHandler(pandaRpcHandler)
    rpcEnv.awaitTermination()
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
    rpcEnv.shutdown()
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

}
