package cn.pandadb.server.internode

import cn.pandadb.network.internal.message.{InternalRpcRequest, InternalRpcResponse}
import cn.pandadb.server.{DataLogDetail, PNodeServerContext}
import cn.pandadb.server.rpc.RequestHandler

/**
  * Created by bluejoe on 2019/11/25.
  */
case class InterNodeRequestHandler() extends RequestHandler {
  override val logic: PartialFunction[InternalRpcRequest, InternalRpcResponse] = {
    case GetLogDetailsRequest(sinceVersion: Int) =>
      GetLogDetailsResponse(PNodeServerContext.getJsonDataLog.consume(logItem => logItem, sinceVersion).toArray)
  }
}

case class GetLogDetailsRequest(sinceVersion: Int) extends InternalRpcRequest {

}

case class GetLogDetailsResponse(logs: Array[DataLogDetail]) extends InternalRpcResponse {

}