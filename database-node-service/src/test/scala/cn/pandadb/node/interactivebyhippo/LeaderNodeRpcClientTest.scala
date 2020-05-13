package cn.pandadb.node.interactivebyhippo

import cn.pandadb.leadernode.{LeaderNodeDriver, LeaderSayHello}
import cn.pandadb.util.PandaReplyMessage
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import org.junit.Test

import scala.concurrent.Await
import scala.concurrent.duration.Duration

// before test, should run test package node.interactive's DataNode1 and DataNode2
class LeaderNodeRpcClientTest {
  val leaderDriver = new LeaderNodeDriver

  @Test
  def sayHello(): Unit = {
    val clientConfig = RpcEnvClientConfig(new RpcConf(), "panda-client")
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val endpointRef = clientRpcEnv.setupEndpointRef(new RpcAddress("localhost", 7777), "leader-server")
    val res = Await.result(endpointRef.askWithBuffer[PandaReplyMessage.Value](LeaderSayHello("hello")), Duration.Inf)
    println(res)
    clientRpcEnv.stop(endpointRef)
    clientRpcEnv.shutdown()
  }
}
