package cn.pandadb.server.neo4j

import cn.pandadb.network.internal.message.{InternalRpcRequest, InternalRpcResponse}
import cn.pandadb.server.rpc.{NettyRpcServer, RequestHandler}

/**
  * Created by bluejoe on 2019/11/25.
  */
case class Neo4jRequestHandler() extends RequestHandler {
  override val logic: PartialFunction[InternalRpcRequest, InternalRpcResponse] = {
    //example code
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