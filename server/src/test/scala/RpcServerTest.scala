import cn.pandadb.network.internal.message.{InternalRpcRequest, InternalRpcResponse}
import cn.pandadb.server.rpc.{NettyRpcServer, RequestHandler}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnv, RpcEnvClientConfig}
import org.junit.Test

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Created by bluejoe on 2020/1/7.
  */
class RpcServerTest {
  @Test
  def test1(): Unit = {
    val client = new ExampleClient("localhost", 1234);
    //scalastyle:off println
    println(s"square(101)=${client.square(101)}")
  }
}

object StartExampleRpcServer {
  def main(args: Array[String]) {
    val serverKernel = new NettyRpcServer("0.0.0.0", 1234, "ExampleRPC");
    serverKernel.accept(ExampleRequestHandler());
    serverKernel.start({
      //scalastyle:off println
      println(s"rpc server started!");
    })
  }
}

class ExampleClient(host: String, port: Int) {
  val rpcConf = new RpcConf()
  val config = RpcEnvClientConfig(rpcConf, "ExampleClient")
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

  val endPointRef = rpcEnv.setupEndpointRef(RpcAddress(host, port), "ExampleClient")

  def close(): Unit = {
    rpcEnv.stop(endPointRef)
  }

  def square(x: Double): Double =
    Await.result(endPointRef.ask[CalcSquareResponse](CalcSquareRequest(x)), Duration.Inf).y;
}

case class ExampleRequestHandler() extends RequestHandler {
  override val logic: PartialFunction[InternalRpcRequest, InternalRpcResponse] = {
    case CalcSquareRequest(x: Double) =>
      CalcSquareResponse(x * x)
  }
}

case class CalcSquareRequest(x: Double) extends InternalRpcRequest {

}

case class CalcSquareResponse(y: Double) extends InternalRpcResponse {

}