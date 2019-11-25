package cn.pandadb.server.internode

import cn.pandadb.network.internal.message.{InternalRpcRequest, InternalRpcResponse}
import cn.pandadb.server.LogDetail
import cn.pandadb.server.rpc.NettyRpcServer
import net.neoremind.kraps.rpc.RpcCallContext

/**
  * Created by bluejoe on 2019/11/25.
  */
class InterNodeRpcServer(host: String, port: Int) extends
  NettyRpcServer(host: String, port: Int, "inter-node-server") {
  override def receiveAndReply(context: RpcCallContext): PartialFunction[InternalRpcRequest, InternalRpcResponse] = {
    //example code
    case GetLogDetailsRequest(sinceVersion: Int) =>
      GetLogDetailsResponse(Array())
  }
}

case class GetLogDetailsRequest(sinceVersion: Int) extends InternalRpcRequest {

}

case class GetLogDetailsResponse(logs: Array[LogDetail]) extends InternalRpcResponse {

}