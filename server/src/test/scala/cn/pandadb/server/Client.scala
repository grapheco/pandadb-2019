package cn.pandadb.server

import cn.pandadb.cluster.ClusterService
import cn.pandadb.configuration.Config
import cn.pandadb.leadernode.LeaderNodeDriver
import cn.pandadb.util.PandaReplyMsg
import cn.pandadb.zk.ZKTools
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.neo4j.csv.reader.Extractors.DurationExtractor

import scala.concurrent.duration.Duration

object Client {
  def main(args: Array[String]): Unit = {
    val leaderDriver = new LeaderNodeDriver
    val config = new Config
    val zkTools = new ZKTools(config)
    zkTools.init()
    val clusterService = new ClusterService(config, zkTools)
    clusterService.init()

    val str = clusterService.getLeaderNode().split(":")
    val addr = str(0)
    val port = str(1).toInt
    val clientConfig = RpcEnvClientConfig(new RpcConf(), config.getRpcServerName())
    val clientRpcEnv = HippoRpcEnvFactory.create(clientConfig)
    val ref = clientRpcEnv.setupEndpointRef(new RpcAddress(addr, port), config.getLeaderNodeEndpointName())
    val res = leaderDriver.sayHello("hello", ref, Duration.Inf)
    println(res)
    clientRpcEnv.shutdown()
  }
}
