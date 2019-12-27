package cn.pandadb.server.internode

import java.time.Duration
import java.util

import cn.pandadb.cypherplus.utils.CypherPlusUtils
import cn.pandadb.server.{MainServerContext, DataLogDetail}
import org.neo4j.bolt.runtime.{BoltResult, StatementMetadata, StatementProcessor, TransactionStateMachineSPI}
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark
import org.neo4j.function.{ThrowingBiConsumer, ThrowingConsumer}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.{JavaConversions, mutable}

/**
  * Created by bluejoe on 2019/11/4.
  */
class PNodeStatementProcessor(source: StatementProcessor, spi: TransactionStateMachineSPI) extends StatementProcessor {

  override def markCurrentTransactionForTermination(): Unit = source.markCurrentTransactionForTermination()

  override def commitTransaction(): Bookmark = source.commitTransaction()

  override def run(statement: String, params: MapValue): StatementMetadata = source.run(statement, params)

  override def run(statement: String, params: MapValue, bookmark: Bookmark, txTimeout: Duration,
                   txMetaData: util.Map[String, AnyRef]): StatementMetadata = {

    // param transformation, contribute by codeBabyLin
    val paramMap = new mutable.HashMap[String, AnyRef]()
    val myConsumer = new ThrowingBiConsumer[String, AnyValue, Exception]() {
      override def accept(var1: String, var2: AnyValue): Unit = {
        val key = var1
        val value = ValueUtils.asValue(var2).asObject()
        paramMap.update(key, value)
      }
    }
    params.foreach(myConsumer)
    val mapTrans = JavaConversions.mapAsJavaMap(paramMap)

    //pickup a runnable node
    if (CypherPlusUtils.isWriteStatement(statement)) {
      if (MainServerContext.isLeaderNode) {
        val masterRole = MainServerContext.masterRole
        masterRole.clusterWrite(statement)
      }
      val metaData = source.run(statement, params)
      val curVersion = _getLocalDataVersion() + 1
      _writeDataLog(curVersion, statement)
      metaData
    } else {
      source.run(statement, params)
    }
  }

  private def _getLocalDataVersion(): Int = {
    MainServerContext.dataLogWriter.getLastVersion
  }

  // pandaDB
  private def _writeDataLog(curVersion: Int, cypher: String): Unit = {
    val logItem = new DataLogDetail(curVersion, cypher)
    MainServerContext.dataLogWriter.write(logItem)
  }

  override def streamResult(resultConsumer: ThrowingConsumer[BoltResult, Exception]): Bookmark = {
    source.streamResult(resultConsumer)
  }

  override def hasOpenStatement: Boolean = source.hasOpenStatement

  override def rollbackTransaction(): Unit = source.rollbackTransaction()

  override def hasTransaction: Boolean = source.hasTransaction

  override def reset(): Unit = source.reset()

  override def validateTransaction(): Unit = source.validateTransaction()

  override def beginTransaction(bookmark: Bookmark): Unit = source.beginTransaction(bookmark)

  override def beginTransaction(bookmark: Bookmark, txTimeout: Duration, txMetadata: util.Map[String, AnyRef]): Unit =
    source.beginTransaction(bookmark, txTimeout, txMetadata)

}