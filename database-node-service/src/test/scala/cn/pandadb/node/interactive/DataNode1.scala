package cn.pandadb.node.interactive

import cn.pandadb.configuration.Config
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.RpcEnvServerConfig
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory

object DataNode1 {
  def main(args: Array[String]): Unit = {
    val pandaConfig = new Config
    val serverConfig = RpcEnvServerConfig(new RpcConf(), "server1", "localhost", 6666)
    val serverRpcEnv = HippoRpcEnvFactory.create(serverConfig)
    val endpoint = new DataNodeRpcEndpoint1(serverRpcEnv, pandaConfig)
    serverRpcEnv.setupEndpoint("server1", endpoint)
    serverRpcEnv.setRpcHandler(endpoint)
    serverRpcEnv.awaitTermination()
  }
}
