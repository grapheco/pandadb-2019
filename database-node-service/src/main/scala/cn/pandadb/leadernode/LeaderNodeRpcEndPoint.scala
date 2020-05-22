package cn.pandadb.leadernode

import java.nio.ByteBuffer

import net.neoremind.kraps.rpc._
import org.slf4j.Logger
import cn.pandadb.configuration.{Config => PandaConfig}
import org.grapheco.hippo.{HippoRpcHandler, ReceiveContext}
import cn.pandadb.cluster.ClusterService


class LeaderNodeRpcEndPoint(override val rpcEnv: RpcEnv, pandaConfig: PandaConfig, clusterService: ClusterService)
  extends RpcEndpoint with HippoRpcHandler {

  val logger: Logger = pandaConfig.getLogger(this.getClass)
//  val leaderNodeService = new LeaderNodeServiceImpl

  override def onStart(): Unit = {
    logger.info("start LeaderNodeRpcEndPoint")
  }

  override def onStop(): Unit = {
    logger.info("stop LeaderNodeRpcEndPoint")
  }

}
