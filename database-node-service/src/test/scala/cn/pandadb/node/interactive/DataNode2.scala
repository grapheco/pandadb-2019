package cn.pandadb.node.interactive

import cn.pandadb.configuration.Config
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.RpcEnvServerConfig
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory

object DataNode2 {
  def main(args: Array[String]): Unit = {
    val pandaConfig = new Config
    val serverConfig = RpcEnvServerConfig(new RpcConf(), "server2", "localhost", 6667)
    val serverRpcEnv = HippoRpcEnvFactory.create(serverConfig)
    val endpoint = new DataNodeRpcEndpoint2(serverRpcEnv, pandaConfig)
    serverRpcEnv.setupEndpoint("server2", endpoint)
    serverRpcEnv.setRpcHandler(endpoint)
    serverRpcEnv.awaitTermination()
  }
}

