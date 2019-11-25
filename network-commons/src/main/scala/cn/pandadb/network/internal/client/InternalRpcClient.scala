package cn.pandadb.network.internal.client

import cn.pandadb.network.internal.message.InternalRpcRequest
import cn.pandadb.util.Logging
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnv, RpcEnvClientConfig}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2019/11/25.
  */
class InternalRpcClient(rpcEnv: RpcEnv, host: String, port: Int) extends Logging {
  val endPointRef = {
    val rpcConf = new RpcConf()
    val config = RpcEnvClientConfig(rpcConf, "pandadb-internal-client")
    val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

    rpcEnv.setupEndpointRef(RpcAddress(host, port), "pandadb-internal-client-service")
  }

  def close(): Unit = {
    rpcEnv.stop(endPointRef)
  }

  def request[T >: InternalRpcRequest](message: Any): T = {
    Await.result(endPointRef.ask(message), Duration.Inf);
  }
}
