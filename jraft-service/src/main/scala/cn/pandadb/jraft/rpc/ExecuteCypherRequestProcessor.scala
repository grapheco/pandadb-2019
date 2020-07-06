package cn.pandadb.jraft.rpc

import cn.pandadb.jraft.{PandaJraftService, PandadbJraftClosure}
import com.alipay.sofa.jraft.Status
import com.alipay.sofa.jraft.rpc.{RpcContext, RpcProcessor}

class ExecuteCypherRequestProcessor(pandaJraftService: PandaJraftService) extends RpcProcessor[ExecuteCypherRequest] {
  override def handleRequest(rpcCtx: RpcContext, request: ExecuteCypherRequest): Unit = {
    val pdClosure = new PandadbJraftClosure {
      override def run(status: Status): Unit = {
        rpcCtx.sendResponse(getValueResponse)
      }
    }
    if (request.isReadRequest()) this.pandaJraftService.executeRCypher(request.getCypher(), pdClosure)
    else this.pandaJraftService.executeWCypher(request.getCypher(), pdClosure)
  }

  override def interest(): String = {
    ExecuteCypherRequestProcessor.super.getClass.getName
  }
}
