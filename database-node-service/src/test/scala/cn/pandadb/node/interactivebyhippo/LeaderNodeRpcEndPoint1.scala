package cn.pandadb.node.interactivebyhippo

import java.nio.ByteBuffer

import cn.pandadb.configuration.{Config => PandaConfig}
import cn.pandadb.leadernode.LeaderSayHello
import net.neoremind.kraps.rpc._
import org.grapheco.hippo.{HippoRpcHandler, ReceiveContext}
import org.slf4j.Logger


class LeaderNodeRpcEndPoint1(override val rpcEnv: RpcEnv, pandaConfig: PandaConfig)
  extends RpcEndpoint with HippoRpcHandler {

  val logger: Logger = pandaConfig.getLogger(this.getClass)
  val leaderNodeService = new LeaderNodeServiceImpl1

  override def onStart(): Unit = {
    logger.info("start LeaderNodeRpcEndPoint")
  }


  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case LeaderSayHello(msg) => {
      val res = leaderNodeService.sayHello()
      context.reply(res)
    }
  }


  override def onStop(): Unit = {
    logger.info("stop LeaderNodeRpcEndPoint")
  }

}
