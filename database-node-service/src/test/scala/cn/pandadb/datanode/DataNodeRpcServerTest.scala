package cn.pandadb.datanode

import cn.pandadb.configuration.Config
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.RpcEnvServerConfig
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory

object DataNodeRpcServerTest {
  def main(args: Array[String]): Unit = {
    val pandaConfig = new Config
    val serverConfig = RpcEnvServerConfig(new RpcConf(), "server", "localhost", 6666)
    val serverRpcEnv = HippoRpcEnvFactory.create(serverConfig)
    val endpoint = new DataNodeRpcEndpoint(serverRpcEnv, pandaConfig)
    val handler = new DataNodeHandler(pandaConfig)
    serverRpcEnv.setupEndpoint("server", endpoint)
//    serverRpcEnv.setRpcHandler(handler)
    serverRpcEnv.awaitTermination()
  }
}
