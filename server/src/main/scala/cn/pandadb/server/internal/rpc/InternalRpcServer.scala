package cn.pandadb.server.internal.rpc

import cn.pandadb.network.internal.message.InternalRpcMessage
import cn.pandadb.server.LogDetail
import cn.pandadb.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}

/**
  * Created by bluejoe on 2019/11/25.
  */
class InternalRpcServer(host: String, port: Int) extends Logging {
  var rpcEnv: RpcEnv = null

  def start() {
    val config = RpcEnvServerConfig(new RpcConf(), "pandadb-internal-server", host, port)
    rpcEnv = NettyRpcEnvFactory.create(config)
    val endpoint: RpcEndpoint = new InternalRpcEndpoint(rpcEnv)
    rpcEnv.setupEndpoint("pandadb-internal-server-service", endpoint)
    rpcEnv.awaitTermination()
  }

  def shutdown(): Unit = {
    if (rpcEnv != null) {
      rpcEnv.shutdown()
    }
  }

  class InternalRpcEndpoint(val rpcEnv: RpcEnv) extends RpcEndpoint with Logging {
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      //example code
      case GetLogDetailsRequest(sinceVersion: Int) =>
        context.reply(GetLogDetailsResponse(Array()))
    }
  }
}

case class GetLogDetailsRequest(sinceVersion: Int) extends InternalRpcMessage {

}

case class GetLogDetailsResponse(logs: Array[LogDetail]) extends InternalRpcMessage {

}