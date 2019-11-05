package org.neo4j.bolt.v1.runtime

import java.time.Duration
import java.util

import cn.graiph.cnode.GNodeSelector
import org.neo4j.bolt.runtime.BoltResult.Visitor
import org.neo4j.bolt.runtime.{BoltResult, StatementMetadata, StatementProcessor}
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark
import org.neo4j.cypher.result.QueryResult
import org.neo4j.driver._
import org.neo4j.function.ThrowingConsumer
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConversions

/**
  * Created by bluejoe on 2019/11/4.
  */
class DispatchedStatementProcessor(source: StatementProcessor, selector: GNodeSelector) extends StatementProcessor {
  var _currentStatementResult: StatementResult = _;

  override def markCurrentTransactionForTermination(): Unit = source.markCurrentTransactionForTermination()

  override def commitTransaction(): Bookmark = source.commitTransaction()

  override def run(statement: String, params: MapValue): StatementMetadata = source.run(statement, params)

  var _currentTransaction: Transaction = _

  override def run(statement: String, params: MapValue, bookmark: Bookmark, txTimeout: Duration, txMetaData: util.Map[String, AnyRef]): StatementMetadata = {
    //pickup a runnable node
    val driver = selector.chooseReadNode();
    val session = driver.session();
    _currentTransaction = session.beginTransaction();
    _currentStatementResult = session.run(statement, params.asInstanceOf[Value]);
    //extract metadata from _currentStatementResult.
    new MyStatementMetadata(_currentStatementResult)
  }

  override def streamResult(resultConsumer: ThrowingConsumer[BoltResult, Exception]): Bookmark = {
    resultConsumer.accept(new MyBoltResult(_currentStatementResult));
    //return bookmark
    new Bookmark(-1);
  }

  class MyBoltResult(result: StatementResult) extends BoltResult {
    override def fieldNames(): Array[String] = JavaConversions.collectionAsScalaIterable(result.keys()).toArray

    override def accept(visitor: Visitor): Unit = {
      //visitor.addMetadata();
      val it = result.stream().iterator();
      while (it.hasNext) {
        val record = it.next();
        visitor.visit(new MyRecord(record));
      }
    }

    override def close(): Unit = _currentTransaction.close()
  }

  class MyRecord(record: Record) extends QueryResult.Record {
    override def fields(): Array[AnyValue] = {
      JavaConversions.collectionAsScalaIterable(record.values()).map {
        value: Value =>
          Values.value(value.asObject()).asInstanceOf[AnyValue]
      }.toArray
    }
  }

  class MyStatementMetadata(result: StatementResult) extends StatementMetadata {
    override def fieldNames(): Array[String] = JavaConversions.collectionAsScalaIterable(result.keys()).toArray
  }

  override def hasOpenStatement: Boolean = source.hasOpenStatement

  override def rollbackTransaction(): Unit = source.rollbackTransaction()

  override def hasTransaction: Boolean = source.hasTransaction

  override def reset(): Unit = source.reset()

  override def validateTransaction(): Unit = source.validateTransaction()

  override def beginTransaction(bookmark: Bookmark): Unit = source.beginTransaction(bookmark)

  override def beginTransaction(bookmark: Bookmark, txTimeout: Duration, txMetadata: util.Map[String, AnyRef]): Unit = source.beginTransaction(bookmark, txTimeout, txMetadata)
}