package cn.pandadb.neo4j

import java.io.File

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

import cn.pandadb.configuration.Config
import cn.pandadb.server.modules.LifecycleServerModule
import cn.pandadb.neo4j.rpc.DBRpcServer


class Neo4jServer(config: Config) extends LifecycleServerModule {
  val logger = config.getLogger(this.getClass)

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
    val rpcServer = new DBRpcServer("127.0.0.1", 52340, logger)
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  def createNode(id: Long, labels: Array[String], properties: Map[String, String]): Unit = {
  }
}

object Neo4jServer {
  def main(args: Array[String]): Unit = {
    val server = new Neo4jServer(new Config)
    server.start()
  }
}