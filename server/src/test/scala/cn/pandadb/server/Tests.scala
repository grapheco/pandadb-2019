package cn.pandadb.server

import java.io.File

import cn.pandadb.configuration.Config
import cn.pandadb.datanode.GetAllDBNodes
import cn.pandadb.driver.values.Node
import cn.pandadb.leadernode.{LeaderCreateNode, LeaderNodeDriver, LeaderSayHello, LeaderSayHello2}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.{HippoRpcEnvFactory, NettyRpcEnvFactory}
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import org.junit.Test

import scala.concurrent.duration.Duration

class Tests {
  val config1 = new Config {
    override def getRpcPort(): Int = 52310
    override def getLocalNeo4jDatabasePath(): String = "output1/db1"
  }

  val config2 = new Config {
    override def getRpcPort(): Int = 52320
    override def getLocalNeo4jDatabasePath(): String = "output2/db2"
  }
  val pandaServer1 = new PandaServer(config1)
  val pandaServer2 = new PandaServer(config2)

  val thread1 = new Thread(new Runnable {
    override def run(): Unit = {
      pandaServer1.start()
    }
  })
  thread1.setDaemon(true)
  thread1.start()
  println("===========================")

  val thread2 = new Thread(new Runnable {
    override def run(): Unit = {
      pandaServer2.start()
    }
  })
  thread2.setDaemon(true)
  thread2.start()
  println("++++++++++++++++++++++++++")

  Thread.sleep(10000)

  val leaderNode: String = pandaServer1.clusterService.getLeaderNode()
  println(leaderNode)

  val leaderDriver = new LeaderNodeDriver

  @Test
  def testCreateGraphNode(): Unit = {
    val rpcServerName = config1.getRpcServerName()
    val strs = leaderNode.split(":")
    val leaderHost = strs(0)
    val leaderPort = strs(1).toInt

    val clientConfig = RpcEnvClientConfig(new RpcConf(), "leader-client")
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val endpointRef = clientRpcEnv.setupEndpointRef(new RpcAddress(leaderHost, leaderPort), config1.getLeaderNodeEndpointName())
    val res = endpointRef.askWithRetry[String](LeaderSayHello2("hello"))
    println("=========>>> " + res)

//
//    val clientConfig1 = RpcEnvClientConfig(new RpcConf(), "datanode-client1")
//    val clientRpcEnv1 = HippoRpcEnvFactory.create(clientConfig1)
//    val endpointRef1 = clientRpcEnv1.setupEndpointRef(new RpcAddress(config1.getListenHost(), config1.getRpcPort()),
//      config1.getDataNodeEndpointName())
//    val res3 = endpointRef1.getChunkedStream[Node](GetAllDBNodes(2), Duration.Inf)
//    assert(res3.size == 2)
//
//    val clientConfig2 = RpcEnvClientConfig(new RpcConf(), "datanode-client2")
//    val clientRpcEnv2 = HippoRpcEnvFactory.create(clientConfig2)
//    val endpointRef2 = clientRpcEnv2.setupEndpointRef(new RpcAddress(config2.getListenHost(), config2.getRpcPort()),
//      config2.getDataNodeEndpointName())
//    val res4 = endpointRef2.getChunkedStream[Node](GetAllDBNodes(2), Duration.Inf)
//    assert(res4.size == 2)
  }

}
