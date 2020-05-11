package cn.pandadb.node.interactivebyhippo

import cn.pandadb.configuration.Config
import cn.pandadb.leadernode.LeaderNodeRpcEndPoint
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.RpcEnvServerConfig
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory

object LeaderNodeRpcServerTest {
  def main(args: Array[String]): Unit = {
    val pandaConfig = new Config
    val serverConfig = RpcEnvServerConfig(new RpcConf(), "leader-server", "localhost", 7777)
    val serverRpcEnv = HippoRpcEnvFactory.create(serverConfig)
    val endpoint = new LeaderNodeRpcEndPoint1(serverRpcEnv, pandaConfig)
    serverRpcEnv.setupEndpoint("leader-server", endpoint)
    serverRpcEnv.setRpcHandler(endpoint)
    serverRpcEnv.awaitTermination()
  }
}
