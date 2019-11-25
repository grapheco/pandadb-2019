package cn.pandadb.server.neo4j

import cn.pandadb.network.internal.message.{InternalRpcRequest, InternalRpcResponse}
import cn.pandadb.server.rpc.NettyRpcServer
import net.neoremind.kraps.rpc.RpcCallContext

/**
  * Created by bluejoe on 2019/11/25.
  */
class Neo4jAgent(host: String, port: Int) extends
  NettyRpcServer(host, port, "neo4j-agent") {
  override def receiveAndReply(context: RpcCallContext): PartialFunction[InternalRpcRequest, InternalRpcResponse] = {
    case RunCommandRequest(command: String) =>
      RunCommandResponse(Array())
  }
}


case class RunCommandRequest(command: String) extends InternalRpcRequest {

}

case class RunCommandResponse(results: Array[Result]) extends InternalRpcResponse {

}

class Result {

}