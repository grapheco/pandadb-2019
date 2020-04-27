package cn.pandadb.neo4j.rpc

import java.io.File

import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.neo4j.graphdb.{GraphDatabaseService, Label}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.slf4j.Logger

class DBRpcEndpoint(override val rpcEnv: RpcEnv, log: Logger) extends RpcEndpoint {

  val dbFile = new File("./output/testdb")
  if (!dbFile.exists()) {
    dbFile.mkdirs
  }
  var db: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()

  override def onStart(): Unit = {
    log.info("start DBRpcEndpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case addNode(id, labels, properties) => {
      log.info(s"addNode($id)")
      val tx = db.beginTx()
      val node = db.createNode(id)
      for(labelName <- labels) {
        val label = Label.label(labelName)
        node.addLabel(label)
      }
      properties.map((x)=>{
        node.setProperty(x._1, x._2)
      })
      tx.success()
      tx.close()
      context.reply("Node"+node.getId)
    }
    case getNode(id) => {
      log.info(s"addNode($id)")
      val node = db.getNodeById(id)
      context.reply("Node"+node.getId)
    }
  }

  override def onStop(): Unit = {
    log.info("stop DBRpcEndpoint")
  }
}