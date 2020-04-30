package cn.pandadb.datanode

import cn.pandadb.configuration.Config
import cn.pandadb.leadernode.LeaderNodeRpcEndPoint
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.cluster.ClusterInfoAPI
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.slf4j.Logger


class PandaRpcServer(config: Config) extends LifecycleServerModule {
  val logger: Logger = config.getLogger(this.getClass)

  val rpcHost = config.getListenHost()
  val rpcPort = config.getRpcPort
  val rpcServerName = config.getRpcServerName()

  val rpcConfig = RpcEnvServerConfig(new RpcConf(), rpcServerName, rpcHost, rpcPort)
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(rpcConfig)

  val dataNodeEndpointName = config.getDataNodeEndpointName()
  val dataNodeRpcEndpoint: RpcEndpoint = new DataNodeRpcEndpoint(rpcEnv, config)
  var dataNodeRpcEndpointRef: RpcEndpointRef = null

  val leaderNodeEndpointName = config.getLeaderNodeEndpointName()
  val leaderNodeRpcEndpoint: RpcEndpoint = new LeaderNodeRpcEndPoint(rpcEnv, config)
  var leaderNodeRpcEndpointRef: RpcEndpointRef = null

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
    dataNodeRpcEndpointRef = rpcEnv.setupEndpoint(dataNodeEndpointName, dataNodeRpcEndpoint)
    this.addLeaderNodeRpcEndpoint()
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
    if (ClusterInfoAPI.isLeader) {
      logger.info(this.getClass + ": addLeaderNodeRpcEndpoint")
      leaderNodeRpcEndpointRef = rpcEnv.setupEndpoint(leaderNodeEndpointName, leaderNodeRpcEndpoint)
    }
  }

  def removeLeaderNodeRpcEndpoint(): Unit = {
    if (ClusterInfoAPI.isLeader && leaderNodeRpcEndpointRef != null) {
      logger.info(this.getClass + ": removeLeaderNodeRpcEndpoint")
      rpcEnv.stop(leaderNodeRpcEndpointRef)
    }
  }
}
