package org.neo4j.bolt.v1.runtime

import java.lang.Iterable
import java.time.{Clock, Duration}
import java.util

import cn.graiph.cnode.GNodeSelector
import org.neo4j.bolt.runtime.{BoltResult, StatementMetadata, StatementProcessor}
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark
import org.neo4j.cypher.result.QueryResult
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.driver._
import org.neo4j.function.ThrowingConsumer
import org.neo4j.graphdb.{ExecutionPlanDescription, Notification, QueryExecutionType, QueryStatistics}
import org.neo4j.values.virtual.MapValue

/**
  * Created by bluejoe on 2019/11/4.
  */
class DispatchedStatementProcessor(source: StatementProcessor, selector: GNodeSelector) extends StatementProcessor {
  var _currentStatementResult: StatementResult = _;

  override def markCurrentTransactionForTermination(): Unit = source.markCurrentTransactionForTermination()

  override def commitTransaction(): Bookmark = source.commitTransaction()

  override def run(statement: String, params: MapValue): StatementMetadata = source.run(statement, params)

  override def run(statement: String, params: MapValue, bookmark: Bookmark, txTimeout: Duration, txMetaData: util.Map[String, AnyRef]): StatementMetadata = {
    //pickup a runnable node
    val driver = selector.chooseReadNode();
    val s = driver.session();
    _currentStatementResult = s.run(statement, params.asInstanceOf[Value]);
    //extract metadata from _currentStatementResult.
    null
  }

  override def streamResult(resultConsumer: ThrowingConsumer[BoltResult, Exception]): Bookmark = {
    resultConsumer.accept(new CypherAdapterStream(new QueryResultAdapter(_currentStatementResult), Clock.systemUTC()));
    //return bookmark
    Bookmark.fromParamsOrNull(null);
  }

  override def hasOpenStatement: Boolean = source.hasOpenStatement

  override def rollbackTransaction(): Unit = source.rollbackTransaction()

  override def hasTransaction: Boolean = source.hasTransaction

  override def reset(): Unit = source.reset()

  override def validateTransaction(): Unit = source.validateTransaction()

  override def beginTransaction(bookmark: Bookmark): Unit = source.beginTransaction(bookmark)

  override def beginTransaction(bookmark: Bookmark, txTimeout: Duration, txMetadata: util.Map[String, AnyRef]): Unit = source.beginTransaction(bookmark, txTimeout, txMetadata)
}


class QueryResultAdapter(result: StatementResult) extends QueryResult {
  override def getNotifications: Iterable[Notification] = ???

  override def executionType(): QueryExecutionType = ???

  override def queryStatistics(): QueryStatistics = ???

  override def fieldNames(): Array[String] = ???

  override def accept[E <: Exception](queryResultVisitor: QueryResultVisitor[E]): Unit = ???

  override def executionPlanDescription(): ExecutionPlanDescription = ???

  override def close(): Unit = ???
}