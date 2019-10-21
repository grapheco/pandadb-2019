package cn.graiph.db

import java.util.function.Function
import java.util.stream.Stream

import cn.graiph.driver.CypherService
import org.neo4j.blob.utils.Logging
import org.neo4j.driver._
import org.neo4j.driver.internal.types.InternalMapAccessorWithDefaultValue
import org.neo4j.driver.internal.value.{NodeValue, RelationshipValue}
import org.neo4j.driver.internal.{InternalNode, InternalPair, InternalRelationship}
import org.neo4j.driver.summary.ResultSummary
import org.neo4j.driver.util.Pair
import org.neo4j.graphdb.{GraphDatabaseService, Result}

import scala.collection.JavaConversions
import scala.collection.JavaConversions._

class LocalGraphService(db: GraphDatabaseService)
  extends Logging with CypherService {
  override def queryObjects[T: ClassManifest](queryString: String, fnMap: (Record) => T): Iterator[T] = ???

  override def execute[T](f: (Session) => T): T = {
    throw new UnsupportedOperationException();
  }

  override def executeQuery[T](queryString: String, fn: (StatementResult) => T): T = {
    val tx = db.beginTx();
    val result = db.execute(queryString);
    val t = fn(new StatementResultAdapter(result));
    t;
  }

  override def executeQuery[T](queryString: String, params: Map[String, AnyRef], fn: (StatementResult) => T): T = {
    val tx = db.beginTx();
    val result = db.execute(queryString, params);
    val t = fn(new StatementResultAdapter(result));
    t;
  }

  override def executeUpdate(queryString: String): Unit = {
    val tx = db.beginTx();
    val r = db.execute(queryString);
    r.close();
    tx.success();
  }

  override def executeUpdate(queryString: String, params: Map[String, AnyRef]): Unit = {
    val tx = db.beginTx();
    val r = db.execute(queryString, params);
    r.close();
    tx.success();
  }

  class RecordAdapter(result: Result, raw: Map[String, AnyRef]) extends InternalMapAccessorWithDefaultValue with Record {

    val list: List[(String, Value)] = result.columns().map(x => x -> {
      raw(x) match {
        case m: org.neo4j.graphdb.Node => new NodeValue(new InternalNode(m.getId, m.getLabels.map(_.name()),
          JavaConversions.mapAsJavaMap(m.getAllProperties.map(x =>
            x._1 -> Values.value(x._2)).toMap)))

        case m: org.neo4j.graphdb.Relationship => new RelationshipValue(new InternalRelationship(m.getId, m.getStartNodeId, m.getEndNodeId,
          m.getType.name(),
          JavaConversions.mapAsJavaMap(m.getAllProperties.map(x =>
            x._1 -> Values.value(x._2)).toMap)))

        case m => Values.value(m)
      }
    }
    ).toList

    override def asMap(): java.util.Map[String, AnyRef] = raw

    override def asMap[T](mapper: Function[Value, T]): java.util.Map[String, T] =
      JavaConversions.mapAsJavaMap(
        list.map(x =>
          x._1 -> mapper.apply(x._2)).toMap
      )

    override def values(): java.util.List[Value] = list.map(_._2)

    override def get(key: String): Value = list.find(_._1.equals(key)).get._2

    override def get(index: Int): Value = list(index)._2

    override def size(): Int = raw.size

    override def fields(): java.util.List[Pair[String, Value]] =
      JavaConversions.seqAsJavaList(
        list.map(x =>
          InternalPair.of[String, Value](x._1, x._2))
      )

    override def keys(): java.util.List[String] = result.columns()

    override def containsKey(key: String): Boolean = raw.contains(key)

    override def index(key: String): Int = list.indexWhere(_._1.equals(key))
  }

  class StatementResultAdapter(result: Result) extends StatementResult {
    override def next(): Record = new RecordAdapter(result, JavaConversions.mapAsScalaMap(result.next()).toMap)

    override def consume(): ResultSummary = throw new UnsupportedOperationException()

    override def peek(): Record = throw new UnsupportedOperationException()

    override def summary(): ResultSummary = throw new UnsupportedOperationException()

    override def hasNext: Boolean = result.hasNext

    override def keys(): java.util.List[String] = result.columns()

    override def list(): java.util.List[Record] = this.toList

    override def list[T](mapFunction: Function[Record, T]): java.util.List[T] = list().map(mapFunction.apply(_))

    override def single(): Record =
      if (hasNext)
        null;
      else
        next();

    override def stream(): Stream[Record] = {
      //TODO
      throw new UnsupportedOperationException();
    }
  }

}