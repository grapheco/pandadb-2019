package cn.pandadb.server.driver

import cn.pandadb.server.Bootstrapper
import cn.pandadb.server.Config
import cn.pandadb.server.util.Logging
import cn.pandadb.server.PandaModule

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.RpcEnv
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
//import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.HippoEndpointRef
import net.neoremind.kraps.rpc.{RpcEndpoint, RpcEnvServerConfig}

class Server extends PandaModule {
  val serviceName = "pandaDriverRpc"
  var rpcEnv: RpcEnv = null
  var rpcEndpoint: RpcEndpoint = null

  override def init(config: Config): Unit = {}

  def start(config: Config): Unit = {
    val listenHost: String = config.getListenHost()
    val listenPort: Int = config.getRpcPort
    val rpcConfig = RpcEnvServerConfig(new RpcConf(), serviceName, listenHost, listenPort)
    rpcEnv = NettyRpcEnvFactory.create(rpcConfig)
    rpcEndpoint = new PandaRpcEndpoint(rpcEnv)
    rpcEnv.setupEndpoint(serviceName, rpcEndpoint)
    rpcEnv.awaitTermination()
  }

  def stop(): Unit = {
    if (rpcEndpoint != null) {
      rpcEndpoint.stop()
    }
  }

}


class PandaRpcEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint with Logging{
  override def onStart(): Unit = {
    logger.info("pandaDriverRpc Server start...")
  }

  override def onStop(): Unit = {
    logger.info("pandaDriverRpc Server stop... ")
  }
}

