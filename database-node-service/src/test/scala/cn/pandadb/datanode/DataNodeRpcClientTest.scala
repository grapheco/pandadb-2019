package cn.pandadb.datanode

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
    val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(
      pandaConfig.getListenHost(), pandaConfig.getRpcPort()), pandaConfig.getRpcEndpointName())

    val res = endPointRef.askWithRetry[String](createNode(Array("user", "data"), Map("p1" -> "v1", "p2" -> "v2")))
    println(res)
  }

}
