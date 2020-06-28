package cn.pandadb.datanode

import scala.collection.JavaConverters._

import java.util.concurrent.{BlockingQueue, ConcurrentHashMap, Executors, LinkedBlockingQueue}

import cn.pandadb.configuration.Config
import cn.pandadb.driver.values.{Node => PandaNode}
import cn.pandadb.util.{PandaReplyMessage, ValueConverter}
import org.neo4j.graphdb.{GraphDatabaseService, Label, Node, Transaction}
import org.neo4j.kernel.impl.core.NodeProxy

import scala.collection.mutable.{ArrayBuffer, HashMap}
import util.control.Breaks._


trait DataNodeTxService {

  def beginTx(txId: Long): Transaction;

  def commitTx(txId: Long, isSuccess: Boolean = true): Boolean;

  def closeTx(txId: Long): Boolean;

  def createNodeInTx(txId: Long, id: Long, labels: Array[String], properties: Map[String, Any]): PandaNode;

//  def addNodeLabel(id: Long, label: String): PandaReplyMessage.Value
//
//  def getNodeById(id: Long): Node
//
//  def getNodesByProperty(label: String, propertiesMap: Map[String, Object]): ArrayBuffer[Node]
//
//  def getNodesByLabel(label: String): ArrayBuffer[Node]
//
//  def setNodeProperty(id: Long, propertiesMap: Map[String, Any]): PandaReplyMessage.Value
//
//  def removeNodeLabel(id: Long, toDeleteLabel: String): PandaReplyMessage.Value
//
//  def deleteNode(id: Long): PandaReplyMessage.Value
//
//  def removeNodeProperty(id: Long, property: String): PandaReplyMessage.Value
}

class DataNodeTxServiceImpl(localDatabase: GraphDatabaseService, config: Config) extends DataNodeTxService {

  val txOperationsMap = (new ConcurrentHashMap[Long, LinkedBlockingQueue[OperationInTx]]()).asScala
  val txOperationResultsMap = (new ConcurrentHashMap[Long, LinkedBlockingQueue[Any]]()).asScala
  val txHandlerThreadPool = Executors.newCachedThreadPool()
  val logger = config.getLogger(this.getClass)

  trait OperationInTx {
    def execute(outputs: BlockingQueue[Any],
                localNeo4jDB: GraphDatabaseService): Unit;
  }
  case class SuccessInTx() extends OperationInTx {
    def execute(outputs: BlockingQueue[Any],
                localNeo4jDB: GraphDatabaseService): Unit = {}
  }
  case class FailureInTx() extends OperationInTx {
    def execute(outputs: BlockingQueue[Any],
                localNeo4jDB: GraphDatabaseService): Unit = {}
  }
  case class CloseInTx() extends OperationInTx {
    def execute(outputs: BlockingQueue[Any],
                localNeo4jDB: GraphDatabaseService): Unit = {}
  }

  class TxHandler(inputs: BlockingQueue[OperationInTx], outputs: BlockingQueue[Any],
                  localDatabase: GraphDatabaseService) extends Runnable {
    override def run(): Unit = {
      val tx = localDatabase.beginTx()
      outputs.put(tx)
      breakable(
        while (true) {
          val op: OperationInTx = inputs.take()
          logger.info("take one operation : " + op.toString )
          op match {
            case op: SuccessInTx => { tx.success(); outputs.put(true); }
            case op: FailureInTx => { tx.failure(); outputs.put(true);}
            case op: CloseInTx => { tx.close(); outputs.put(true); break(); }
            case op => { op.execute(outputs, localDatabase) }
          }
        }
      )
    }
  }

  override def beginTx(txId: Long): Transaction = {
    // bind pandadb transaction and neo4j transaction through txId
    // init inputs and outputs BlockingQueue
    txOperationsMap(txId) = new LinkedBlockingQueue[OperationInTx]()
    txOperationResultsMap(txId) = new LinkedBlockingQueue[Any]()
    // create txHandler thread
    txHandlerThreadPool.execute(new TxHandler(txOperationsMap(txId), txOperationResultsMap(txId), localDatabase))
    val transaction = txOperationResultsMap(txId).take()
    assert(transaction.isInstanceOf[Transaction], "Expect Type of org.neo4j.graphdb.Transaction")
    logger.info("begin tx: " + transaction.getClass)
    transaction.asInstanceOf[Transaction]
  }

  override def commitTx(txId: Long, isSuccess: Boolean = true): Boolean = {
    if (!txOperationsMap.contains(txId)) {
      return false
    }

    // add operation to TxHandler Thread inputs
    if (isSuccess) {
      txOperationsMap(txId).put(SuccessInTx())
    }
    else {
      txOperationsMap(txId).put(FailureInTx())
    }
    val res = txOperationResultsMap(txId).take()
    assert(res.isInstanceOf[Boolean], "Expect Type of Boolean")
    logger.info("commit tx: " + res)
    res.asInstanceOf[Boolean]
  }

  override def closeTx(txId: Long): Boolean = {
    if (!txOperationsMap.contains(txId)) {
      return false
    }

    txOperationsMap(txId).put(CloseInTx())
    val res = txOperationResultsMap(txId).take()
    assert(res.isInstanceOf[Boolean], "Expect Type of Boolean")
    logger.info("close tx: " + res)
    res.asInstanceOf[Boolean]
  }

  override def createNodeInTx(txId: Long, id: Long, labels: Array[String], properties: Map[String, Any]): PandaNode = {
    txOperationsMap(txId).put(new OperationInTx {
      override def execute(outputs: BlockingQueue[Any], localNeo4jDB: GraphDatabaseService): Unit = {
        val node = localDatabase.createNode(id)
        for (labelName <- labels) {
          val label = Label.label(labelName)
          node.addLabel(label)
        }
        properties.foreach(x => {
          node.setProperty(x._1, x._2)
        })
        val pandaNode: PandaNode = ValueConverter.toDriverNode(node)
        outputs.put(pandaNode)
      }
    })

    val createdNode = txOperationResultsMap(txId).take()
    assert(createdNode.isInstanceOf[PandaNode], "Expect Type of cn.pandadb.driver.values.Node")
    logger.info("create Node: " + createdNode)
    createdNode.asInstanceOf[PandaNode]
  }

}
