package cn.pandadb.datanode

import cn.pandadb.configuration.Config
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.RpcEnvServerConfig
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory

object DataNodeRpcServerTest {
  def main(args: Array[String]): Unit = {
    val pandaConfig = new Config
    val serverConfig = RpcEnvServerConfig(new RpcConf(), "server", "localhost", 12345)
    val serverRpcEnv = HippoRpcEnvFactory.create(serverConfig)
    val endpoint = new DataNodeRpcEndpoint(serverRpcEnv, pandaConfig)
    serverRpcEnv.setupEndpoint("server", endpoint)
    serverRpcEnv.setRpcHandler(endpoint)
    serverRpcEnv.awaitTermination()
  }
}
