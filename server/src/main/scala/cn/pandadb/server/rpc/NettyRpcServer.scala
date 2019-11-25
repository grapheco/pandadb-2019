package cn.pandadb.server.rpc

import cn.pandadb.network.internal.message.{InternalRpcRequest, InternalRpcResponse}
import cn.pandadb.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}

/**
  * Created by bluejoe on 2019/11/25.
  */
abstract class NettyRpcServer(host: String, port: Int, serverName: String) extends Logging {
  val config = RpcEnvServerConfig(new RpcConf(), serverName, host, port)
  val thisRpcEnv = NettyRpcEnvFactory.create(config)
  val endpoint: RpcEndpoint = new RpcEndpoint() {
    override val rpcEnv: RpcEnv = thisRpcEnv;
    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case x: InternalRpcRequest =>
      context.reply(receiveAndReply(context));
    }
  }

  def start() {
    thisRpcEnv.setupEndpoint(s"$serverName-end-point", endpoint)
    thisRpcEnv.awaitTermination()
  }

  def receiveAndReply(context: RpcCallContext): PartialFunction[InternalRpcRequest, InternalRpcResponse];

  def shutdown(): Unit = {
    thisRpcEnv.shutdown()
  }
}
