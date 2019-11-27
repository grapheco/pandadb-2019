package cn.pandadb.driver


import java.util

import cn.pandadb.cypherplus.utils.CypherPlusUtils
import cn.pandadb.network.{ClusterClient, NodeAddress}
import org.neo4j.driver.internal.{AbstractStatementRunner, SessionConfig}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Record, Session, Statement, StatementResult, StatementRunner, Transaction, TransactionConfig, Value, Values}

import scala.collection.mutable.ArrayBuffer


/**
 * @Author: codeBabyLin
 * @Description:
 * @Date: Created in 9:06 2019/11/26
 * @Modified By:
 */


class PandaTransaction(sessionConfig: SessionConfig, config: TransactionConfig, clusterOperator: ClusterClient) extends Transaction{

  var transactionArray: ArrayBuffer[Transaction] = ArrayBuffer[Transaction]()  //save session
  var sessionArray: ArrayBuffer[Session] = ArrayBuffer[Session]()   // save transaction

  var transaction: Transaction = _
  var session: Session = _
  var readDriver: Driver = _
  var writeDriver : Driver = _
  //rule1 one session ,one transaction
  //rule2 session closed,transaction does't work
  private def getSession(isWriteStatement: Boolean): Session = {
    //if (!(this.session==null)) this.session.close()   //session the sanme with the Transaction can not close
    if (isWriteStatement) {
      if (this.writeDriver==null) this.writeDriver = SelectNode.getDriver(isWriteStatement, clusterOperator)
      this.session = this.writeDriver.session(sessionConfig)
    } else {
      if (this.readDriver==null) this.readDriver = SelectNode.getDriver(isWriteStatement, clusterOperator)
      this.session = this.readDriver.session(sessionConfig)
    }
    this.session
  }
  private def getTransactionReady(isWriteStatement: Boolean): Transaction = {
    this.session = getSession(isWriteStatement)
    this.transaction = session.beginTransaction(config)
    this.sessionArray += this.session
    this.transactionArray += this.transaction
    this.transaction
  }




  override def success(): Unit = {
    if (this.transactionArray.nonEmpty) this.transactionArray.foreach(trans => trans.success())
  }

  override def failure(): Unit = {
    if (this.transactionArray.nonEmpty) this.transactionArray.foreach(trans => trans.failure())
  }

  override def close(): Unit = {
    if (this.transactionArray.nonEmpty) this.transactionArray.foreach(trans => trans.close())
    if (this.sessionArray.nonEmpty) this.sessionArray.foreach(sess => sess.close())
    if (!(this.writeDriver == null)) this.writeDriver.close()
    if (!(this.readDriver == null)) this.readDriver.close()
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
    //transanction could not be closed until
    val tempState = statement.text().toLowerCase()
    val isWriteStatement = CypherPlusUtils.isWriteStatement(tempState)
    getTransactionReady(isWriteStatement)
    this.transaction.run(statement)
  }

  override def isOpen: Boolean = {
    if (!(this.transaction == null)) this.transaction.isOpen else true
  }
}
