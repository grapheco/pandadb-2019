package cn.pandadb.leadernode

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.junit.Test

import cn.pandadb.configuration.Config


class DataNodeRpcClientTest {

  @Test
  def test(): Unit = {
    val pandaConfig = new Config
    val rpcConf = new RpcConf()
    val rpcConfig = RpcEnvClientConfig(rpcConf, "panda-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(rpcConfig)
    val leaderNodeEndPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(
      pandaConfig.getListenHost(), pandaConfig.getRpcPort()), pandaConfig.getLeaderNodeEndpointName())

    val res = leaderNodeEndPointRef.askWithRetry[String](createNode( Array("user", "data"), Map("p1" -> "v1", "p2" -> "v2")))
    println(res)
  }

}
