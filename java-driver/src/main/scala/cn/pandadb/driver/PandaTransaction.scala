package cn.pandadb.driver


import java.util

import cn.pandadb.cypherplus.utils.CypherPlusUtils
import cn.pandadb.network.{ClusterClient, NodeAddress}
import org.neo4j.driver.internal.{AbstractStatementRunner, SessionConfig}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Record, Session, Statement, StatementResult, StatementRunner, Transaction, TransactionConfig, Value, Values}


class PandaTransaction(sessionConfig: SessionConfig, config: TransactionConfig, clusterOperator: ClusterClient) extends Transaction{

  var isSucess = false
  var isFailue = false
  var transaction: Transaction = null
  var session: Session = null
  var driver: Driver = null
  private def isRead(statement: String): Boolean = {
    val tempStatement = statement.toLowerCase()
    if (CypherPlusUtils.isWriteStatement(tempStatement)) {
      false
    }
    else true
  }
  private def getWriteNode(): NodeAddress = {
    //clusterOperator.getWriteMasterNode()
    val hos = "10.0.86.179"
    val por = 7687
    new NodeAddress(hos, por)
  }
  private def getReadNode(): NodeAddress = {
    //random to pick up a node
    //val nodeLists = clusterOperator.getAllNodes().toList
    //val index = (new util.Random).nextInt(nodeLists.length)
    // nodeLists(index)
    val hos = "10.0.86.179"
    val por = 7687
    new NodeAddress(hos, por)
  }
  private def getNodeByStatement(statement: String): NodeAddress = {
    if (isRead(statement)) getReadNode() else getWriteNode()
  }
  private def getTransactionReady(node: NodeAddress): Transaction = {
    val host = node.host
    val port = node.port
    val uri = s"bolt://$host:$port"
    this.driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "123456"))
    //val driver = GraphDatabase.driver(uri, AuthTokens.basic("", ""))
    this.session = driver.session(sessionConfig)
    this.transaction = session.beginTransaction(config)
    if(isSucess) this.transaction.success()
    if(isFailue) this.transaction.failure()
    this.transaction
  }




  override def success(): Unit = {
    isSucess = true
  }

  override def failure(): Unit = {
    isFailue = true
  }

  override def close(): Unit = {
    if (!(this.transaction == null)) transaction.close()
    if (!(this.session == null)) session.close()
    if (!(this.driver == null)) driver.close()
  }

  override def run(s: String, value: Value): StatementResult = {
    this.run(new Statement(s, value))
  }

  override def run(s: String, map: util.Map[String, AnyRef]): StatementResult = {
    this.run(s, AbstractStatementRunner.parameters(map))
  }

  override def run(s: String, record: Record): StatementResult = {
    this.run(s, AbstractStatementRunner.parameters(record))
  }

  override def run(s: String): StatementResult = {
    this.run(s, Values.EmptyMap)
  }

  override def run(statement: Statement): StatementResult = {
    getTransactionReady(getNodeByStatement(statement.text()))
    this.transaction.run(statement)
  }

  override def isOpen: Boolean = {
    true
  }
}
