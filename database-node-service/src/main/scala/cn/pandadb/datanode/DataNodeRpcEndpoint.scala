package cn.pandadb.datanode

import net.neoremind.kraps.rpc._
import org.slf4j.Logger
import cn.pandadb.configuration.{Config => PandaConfig}


class DataNodeRpcEndpoint(override val rpcEnv: RpcEnv, pandaConfig: PandaConfig) extends RpcEndpoint {

  val logger: Logger = pandaConfig.getLogger(this.getClass)


  override def onStart(): Unit = {
    logger.info("start DataNodeRpcEndpoint")
  }

  override def onStop(): Unit = {
    logger.info("stop DataNodeRpcEndpoint")
  }

}
