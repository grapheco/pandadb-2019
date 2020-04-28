package cn.pandadb.datanode

import java.io.File

import cn.pandadb.neo4j.rpc.DBRpcEndpoint
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.neo4j.graphdb.{GraphDatabaseService, Label}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger


class DataNodeRpcEndpoint(override val rpcEnv: RpcEnv, log: Logger, dataNodeService: DataNodeService) extends RpcEndpoint {

  override def onStart(): Unit = {
    log.info("start DBRpcEndpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {

    case createNodeWithId(id, labels, properties) => {
      log.info(s"createNodeWithId($id)")
      dataNodeService.createNodeWithId(id, labels, properties)
      context.reply("createNodeWithId")
    }

    case createNode(labels, properties) => {
      log.info(s"createNode()")
      dataNodeService.createNode(labels, properties)
      context.reply("createNode")
    }

    case getNode(id) => {
      log.info(s"addNode($id)")
      dataNodeService.getNode(id)
      context.reply("getNode")
    }

  }

  override def onStop(): Unit = {
    log.info("stop DBRpcEndpoint")
  }

}
