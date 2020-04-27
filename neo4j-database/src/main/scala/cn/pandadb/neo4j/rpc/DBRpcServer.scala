package cn.pandadb.neo4j.rpc

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.slf4j.Logger

class DBRpcServer( host: String, port: Int, log: Logger) {
  val config = RpcEnvServerConfig(new RpcConf(), "neo4j-db-server", host, port)
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
  val dbEndpoint: RpcEndpoint = new DBRpcEndpoint(rpcEnv, log)
  rpcEnv.setupEndpoint("neo4j-db-service", dbEndpoint)
  rpcEnv.awaitTermination()
}
