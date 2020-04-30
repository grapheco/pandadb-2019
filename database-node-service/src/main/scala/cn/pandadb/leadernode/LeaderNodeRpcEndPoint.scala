package cn.pandadb.leadernode

import net.neoremind.kraps.rpc._
import org.slf4j.Logger
import cn.pandadb.configuration.{Config => PandaConfig}


class LeaderNodeRpcEndPoint(override val rpcEnv: RpcEnv, pandaConfig: PandaConfig) extends RpcEndpoint {

  val logger: Logger = pandaConfig.getLogger(this.getClass)
  val leaderNodeService = new LeaderNodeServiceImpl

  override def onStart(): Unit = {
    logger.info("start LeaderNodeRpcEndPoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case createNode(labels, properties) => {
      logger.info(s"createNode()")
      leaderNodeService.createNode(labels, properties)
      context.reply("createNode")
    }

  }

  override def onStop(): Unit = {
    logger.info("stop LeaderNodeRpcEndPoint")
  }

}
