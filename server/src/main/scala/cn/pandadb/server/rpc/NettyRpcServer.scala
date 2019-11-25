package cn.pandadb.server.rpc

import cn.pandadb.network.internal.message.{InternalRpcRequest, InternalRpcResponse}
import cn.pandadb.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by bluejoe on 2019/11/25.
  */
class NettyRpcServer(host: String, port: Int, serverName: String) extends Logging {
  val config = RpcEnvServerConfig(new RpcConf(), serverName, host, port)
  val thisRpcEnv = NettyRpcEnvFactory.create(config)
  val handlers = ArrayBuffer[PartialFunction[InternalRpcRequest, InternalRpcResponse]]();

  val endpoint: RpcEndpoint = new RpcEndpoint() {
    override val rpcEnv: RpcEnv = thisRpcEnv;

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case request: InternalRpcRequest =>
        val response = handlers.find {
          _.isDefinedAt(request)
        }.map(_.apply(request)).get
        context.reply(response)
    }
  }

  def accept(handler: PartialFunction[InternalRpcRequest, InternalRpcResponse]): Unit = {
    handlers += handler;
  }

  def accept(handler: RequestHandler): Unit = {
    handlers += handler.logic;
  }

  def start(onStarted: => Unit = {}) {
    thisRpcEnv.setupEndpoint(s"$serverName-end-point", endpoint)
    onStarted;
    thisRpcEnv.awaitTermination()
  }

  def shutdown(): Unit = {
    thisRpcEnv.shutdown()
  }
}

trait RequestHandler {
  val logic: PartialFunction[InternalRpcRequest, InternalRpcResponse];
}