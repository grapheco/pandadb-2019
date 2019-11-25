package cn.pandadb.server.internode

import cn.pandadb.network.internal.message.{InternalRpcRequest, InternalRpcResponse}
import cn.pandadb.server.LogDetail
import cn.pandadb.server.rpc.{NettyRpcServer, RequestHandler}

/**
  * Created by bluejoe on 2019/11/25.
  */
case class InterNodeRequestHandler() extends RequestHandler {
  override val logic: PartialFunction[InternalRpcRequest, InternalRpcResponse] = {
    //example code
    case GetLogDetailsRequest(sinceVersion: Int) =>
      GetLogDetailsResponse(Array())
  }
}

case class GetLogDetailsRequest(sinceVersion: Int) extends InternalRpcRequest {

}

case class GetLogDetailsResponse(logs: Array[LogDetail]) extends InternalRpcResponse {

}