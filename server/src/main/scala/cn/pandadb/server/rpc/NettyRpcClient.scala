package cn.pandadb.server.rpc

import cn.pandadb.network.NodeAddress
import cn.pandadb.server.DataLogDetail
import cn.pandadb.server.internode.{GetLogDetailsRequest, GetLogDetailsResponse}
import cn.pandadb.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object PNodeRpcClient {
  val rpcEnv: RpcEnv = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "PNodeRpc-client")
    NettyRpcEnvFactory.create(config)
  }

  // if can't connect, wait for it
  def connect(remoteAddress: NodeAddress): PNodeRpcClient = {
    try {
      new PNodeRpcClient(rpcEnv, remoteAddress)
    }
    catch {
      case e: Exception =>
        Thread.sleep(2000)
        connect(remoteAddress)
    }
  }

  def connect(remoteAddress: String): PNodeRpcClient = {
    connect(NodeAddress.fromString(remoteAddress))
  }
}

case class PNodeRpcClient(rpcEnv: RpcEnv, val remoteAddress: NodeAddress) extends Logging {
  val endPointRef = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "PNodeRpc-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    rpcEnv.setupEndpointRef(RpcAddress(remoteAddress.host, remoteAddress.port), "PNodeRpc-service")
  }

  def close(): Unit = {
    rpcEnv.stop(endPointRef)
  }

  def getRemoteLogs(sinceVersion: Int): Array[DataLogDetail] = {
    val response: GetLogDetailsResponse = Await.result(endPointRef.ask[GetLogDetailsResponse](GetLogDetailsRequest(sinceVersion)), Duration.Inf)
    response.logs
  }

}