package org.neo4j.bolt.v1.runtime

import java.time.Duration
import java.{lang, util}

import cn.pandadb.blob.Blob
import cn.pandadb.cnode.GNodeSelector
import org.neo4j.bolt.runtime.BoltResult.Visitor
import org.neo4j.bolt.runtime.{BoltResult, StatementMetadata, StatementProcessor, TransactionStateMachineSPI}
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark
import org.neo4j.cypher.result.QueryResult
import org.neo4j.driver._
import org.neo4j.driver.internal.value.{BooleanValue, BytesValue, FloatValue, IntegerValue, NodeValue, StringValue}
import org.neo4j.function.{ThrowingBiConsumer, ThrowingConsumer}
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Label, Node, Relationship, RelationshipType}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{BlobValue, DoubleValue}
import org.neo4j.values.virtual.{ListValue, MapValue}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

/**
  * Created by bluejoe on 2019/11/4.
  */
class DispatchedStatementProcessor(source: StatementProcessor, spi: TransactionStateMachineSPI, selector: GNodeSelector) extends StatementProcessor {
  var _currentStatementResult: StatementResult = _;

  override def markCurrentTransactionForTermination(): Unit = source.markCurrentTransactionForTermination()

  override def commitTransaction(): Bookmark = source.commitTransaction()

  override def run(statement: String, params: MapValue): StatementMetadata = source.run(statement, params)

  var _currentTransaction: Transaction = _

  override def run(statement: String, params: MapValue, bookmark: Bookmark, txTimeout: Duration, txMetaData: util.Map[String, AnyRef]): StatementMetadata = {

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
    val tempStatement = statement.toLowerCase()
    if (tempStatement.contains("create") || tempStatement.contains("merge") ||
      tempStatement.contains("set") || tempStatement.contains("delete")) {
      //      val driver = selector.chooseWriteNode();
      //      val session = driver.session();
      //      _currentTransaction = session.beginTransaction();
      //      _currentStatementResult =  _currentTransaction.run(statement, mapTrans);
      //      _currentTransaction.success();
      //      _currentTransaction.close();

      val driverList: List[Driver] = selector.chooseAllNodes()
      val closeList: ArrayBuffer[(Session, Transaction)] = new ArrayBuffer[(Session, Transaction)]()
      var tempResult:StatementResult = null
      var tempTransaction:Transaction = null
      try{
        driverList.foreach(driver => {
          val session = driver.session()
          val tx = session.beginTransaction()
          tempTransaction = tx
          tempResult = tx.run(statement, mapTrans)
          closeList += Tuple2(session, tx)
        })
        closeList.foreach(sessionAndTx => {
          sessionAndTx._2.success()
          sessionAndTx._1.close()
        })
        _currentTransaction = tempTransaction
        _currentStatementResult = tempResult
      }catch {
        case e:Exception =>{
          closeList.foreach(sessionAndTx =>{
            sessionAndTx._2.failure()
            sessionAndTx._1.close()
          })
          _currentTransaction = null
          _currentStatementResult = null
        }
      }
    }
    else {
      val driver = selector.chooseReadNode();
      val session = driver.session();
      _currentTransaction = session.beginTransaction();
      _currentStatementResult = _currentTransaction.run(statement, mapTrans);
    }

    //extract metadata from _currentStatementResult.
    new MyStatementMetadata(_currentStatementResult)
  }

  override def streamResult(resultConsumer: ThrowingConsumer[BoltResult, Exception]): Bookmark = {
    resultConsumer.accept(new MyBoltResult(_currentStatementResult));
    //return bookmark
    new Bookmark(spi.newestEncounteredTxId());
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
          value match {
            //TODO: check different types of XxxValue, unpack and use ValueUtils to transform
            case nodeValue: NodeValue => ValueUtils.asAnyValue(new MyDriverNodeToDbNode(nodeValue))
            case intValue: IntegerValue => ValueUtils.asAnyValue(intValue.asInt())
            case floatValue: FloatValue => ValueUtils.asAnyValue(floatValue.asFloat())
            case _ => ValueUtils.asAnyValue(value.asObject())
          }
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

  // class for driver node type transform to DB's node type
  class MyDriverNodeToDbNode(driverNode: NodeValue) extends Node {

    override def getId: Long = driverNode.asEntity().id()

    override def delete(): Unit = {}

    override def getRelationships: lang.Iterable[Relationship] = null

    override def hasRelationship: Boolean = false

    override def getRelationships(types: RelationshipType*): lang.Iterable[Relationship] = null

    override def getRelationships(direction: Direction, types: RelationshipType*): lang.Iterable[Relationship] = null

    override def hasRelationship(types: RelationshipType*): Boolean = false

    override def hasRelationship(direction: Direction, types: RelationshipType*): Boolean = false

    override def getRelationships(dir: Direction): lang.Iterable[Relationship] = null

    override def hasRelationship(dir: Direction): Boolean = false

    override def getRelationships(`type`: RelationshipType, dir: Direction): lang.Iterable[Relationship] = null

    override def hasRelationship(`type`: RelationshipType, dir: Direction): Boolean = false

    override def getSingleRelationship(`type`: RelationshipType, dir: Direction): Relationship = null

    override def createRelationshipTo(otherNode: Node, `type`: RelationshipType): Relationship = null

    override def getRelationshipTypes: lang.Iterable[RelationshipType] = null

    override def getDegree: Int = 0

    override def getDegree(`type`: RelationshipType): Int = 0

    override def getDegree(direction: Direction): Int = 0

    override def getDegree(`type`: RelationshipType, direction: Direction): Int = 0

    override def addLabel(label: Label): Unit = {}

    override def removeLabel(label: Label): Unit = {}

    override def hasLabel(label: Label): Boolean = true

    override def getLabels: lang.Iterable[Label] = {
      val itor = JavaConversions.asScalaIterator(driverNode.asNode().labels().iterator())
      val iter = itor.map(label => Label.label(label))
      JavaConversions.asJavaIterable(iter.toIterable)
    }

    override def getGraphDatabase: GraphDatabaseService = null

    override def hasProperty(key: String): Boolean = false

    override def getProperty(key: String): AnyRef = null

    override def getProperty(key: String, defaultValue: Any): AnyRef = null

    override def setProperty(key: String, value: Any): Unit = {}

    override def removeProperty(key: String): AnyRef = null

    override def getPropertyKeys: lang.Iterable[String] = null

    override def getProperties(keys: String*): util.Map[String, AnyRef] = null

    override def getAllProperties: util.Map[String, AnyRef] = driverNode.asEntity().asMap()
  }

}