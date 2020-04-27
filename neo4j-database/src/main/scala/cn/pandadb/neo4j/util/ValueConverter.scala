package cn.pandadb.neo4j.util

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetTime, ZonedDateTime}
import java.time.temporal.Temporal

import cn.pandadb.neo4j.driver.values.{Duration, Label, Node, NodeValue, Point, Point2D, Relationship, RelationshipType, RelationshipValue, Value}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import org.neo4j.graphdb.{Result => Neo4jResult}
import cn.pandadb.neo4j.driver.result.{InternalRecords, Record}
import cn.pandadb.neo4j.driver.values
import org.neo4j.kernel.impl.core.{NodeProxy, RelationshipProxy}
import org.neo4j.graphdb.Path
import org.neo4j.values.storable.{DurationValue => Neo4jDurationValue, PointValue => Neo4jPointValue}
import org.neo4j.graphdb.{Label => Neo4jLabel, Node => Neo4jNode, Relationship => Neo4jRelationship, RelationshipType => Neo4jType}

import scala.collection.mutable.ArrayBuffer

/*
* neo4j's Value to driver's Value
* */

object ValueConverter {

  def convertValue(v: Any): Value = {
    v match {
      case node: NodeProxy => convertNode(node)
      case relationship: RelationshipProxy => convertRelationship(relationship)
      case path: Path => convertPath(path)
      case i: Int => values.IntegerValue(i)
      case l: Long => values.IntegerValue(l)
      case str: String => values.StringValue(str)
      case d: Double => values.FloatValue(d)
      case bool: Boolean => values.BooleanValue(bool)
      case number: Number => values.NumberValue(number)
      case duration: Neo4jDurationValue => convertDuration(duration)
      case date: LocalDate => values.DateValue(date)
      case time: OffsetTime => values.TimeValue(time)
      case dateTime: ZonedDateTime => values.DateTimeValue(dateTime)
      case localTime: LocalTime => values.LocalTimeValue(localTime)
      case localDateTime: LocalDateTime => values.LocalDateTimeValue(localDateTime)
      case point: Neo4jPointValue => convertPoint(point)
      case map: Map[String, Value] => values.MapValue(map)
      case list: Object => convertList(list)
      case _ => if (v == null) values.NullValue
      else new values.AnyValue(v)
    }
  }

  def convertNode(node: NodeProxy): values.NodeValue = {
    val id = node.getId
    val neo4jLabels = node.getLabels
    var labels = new ArrayBuffer[Label]
    for (l <- neo4jLabels)
      labels += convertLabel(l)
    val props: mutable.Map[String, AnyRef] = node.getAllProperties().asScala
    val propsMap = new mutable.HashMap[String, Value]()
    for (k <- props.keys) {
      val v1 = props.get(k).getOrElse(null)
      val v: Value = convertValue(v1)
      propsMap(k) = v
    }
    val node1 = new values.Node(id, propsMap.toMap, labels.toArray)
    new values.NodeValue(node1)
  }

  def convertRelationship(relationship: RelationshipProxy): values.RelationshipValue = {
    val id = relationship.getId
    val relationshipType = convertType(relationship.getType)
    val props: mutable.Map[String, AnyRef] = relationship.getAllProperties().asScala
    val propsMap = new mutable.HashMap[String, Value]()
    val startNode = convertNode(relationship.getStartNode.asInstanceOf[NodeProxy]).asNode()
    val endNode = convertNode(relationship.getEndNode.asInstanceOf[NodeProxy]).asNode()
    for (k <- props.keys) {
      val v1 = props.get(k).getOrElse(null)
      val v: Value = convertValue(v1)
      propsMap(k) = v
    }
    val rel = new values.Relationship(id, propsMap.toMap, startNode, endNode, relationshipType)
    new values.RelationshipValue(rel)
  }

  def convertPath(path: Path): values.PathValue = {
    var nodes = new ArrayBuffer[Node]
    var relationships = new ArrayBuffer[Relationship]
    val neo4jNodes = path.nodes()
    for (n <- neo4jNodes) nodes += toDriverNode(n)
    val neo4jRelationship = path.relationships()
    for (r <- neo4jRelationship) relationships += toDriverRelationship(r)
    val path1 = new values.Path(nodes.toArray, relationships.toArray)
    new values.PathValue(path1)
  }

  def convertDuration(duration: Neo4jDurationValue): values.DurationValue = {
    val temp = duration.getUnits
    val months = duration.get(temp.get(0))
    val days = duration.get(temp.get(1))
    val seconds = duration.get(temp.get(2))
    val nano = duration.get(temp.get(3))
    val dur = new values.Duration(months, days, seconds, nano)
    new values.DurationValue(dur)
  }

  def convertPoint(point: Neo4jPointValue): values.PointValue = {
    if (point.coordinate().size == 2) {
      val ss = point.getCoordinateReferenceSystem.getCode
      val xx = point.coordinate()(0)
      val yy = point.coordinate()(1)
      val p = new values.Point2D(ss, xx, yy)
      new values.PointValue(p)
    }
    else {
      val ss = point.getCoordinateReferenceSystem.getCode
      val xx = point.coordinate()(0)
      val yy = point.coordinate()(1)
      val zz = point.coordinate()(2)
      val p = new values.Point3D(ss, xx, yy, zz)
      new values.PointValue(p)
    }
  }

  def convertList(list: Object): values.ListValue = {
    var newList = new ArrayBuffer[Value]
    list match {
      case listString: Array[Any] =>
        for (i <- listString) {
          val v = convertValue(i)
          newList += v
        }
      case listDouble: Array[Double] =>
        for (i <- listDouble) {
          val v = convertValue(i)
          newList += v
        }
      case listBool: Array[Boolean] =>
        for (i <- listBool) {
          val v = convertValue(i)
          newList += v
        }
      case listLong: Array[Long] =>
        for (i <- listLong) {
          val v = convertValue(i)
          newList += v
        }
      case listTime: Array[Temporal] =>
        for (i <- listTime) {
          val v = convertValue(i)
          newList += v
        }
    }
    new values.ListValue(newList)
  }

  def convertLabel(label: Neo4jLabel): Label = Label(label.name)

  def convertType(relationshipType: Neo4jType): RelationshipType = RelationshipType(relationshipType.name())

  def toDriverNode(neo4jNode: Neo4jNode): Node = {
    val id = neo4jNode.getId
    val neo4jLabels = neo4jNode.getLabels
    var labels = new ArrayBuffer[Label]
    for (l <- neo4jLabels)
      labels += convertLabel(l)
    val props: mutable.Map[String, AnyRef] = neo4jNode.getAllProperties().asScala
    val propsMap = new mutable.HashMap[String, Value]()
    for (k <- props.keys) {
      val v1 = props.get(k).getOrElse(null)
      val v: Value = convertValue(v1)
      propsMap(k) = v
    }
    new values.Node(id, propsMap.toMap, labels.toArray)
  }

  def toDriverRelationship(neo4jRelationship: Neo4jRelationship): Relationship = {
    val id = neo4jRelationship.getId
    val relationshipType = convertType(neo4jRelationship.getType)
    val props: mutable.Map[String, AnyRef] = neo4jRelationship.getAllProperties().asScala
    val propsMap = new mutable.HashMap[String, Value]()
    val startNode = convertNode(neo4jRelationship.getStartNode.asInstanceOf[NodeProxy]).asNode()
    val endNode = convertNode(neo4jRelationship.getEndNode.asInstanceOf[NodeProxy]).asNode()
    for (k <- props.keys) {
      val v1 = props.get(k).getOrElse(null)
      val v: Value = convertValue(v1)
      propsMap(k) = v
    }
    new values.Relationship(id, propsMap.toMap, startNode, endNode, relationshipType)
  }

  def neo4jResultToDriverRecords(result: Neo4jResult): InternalRecords = {
    val internalRecords = new InternalRecords()

    while (result.hasNext) {
      // Map to Record
      val row: mutable.Map[String, AnyRef] = result.next().asScala
      val map1 = new mutable.HashMap[String, Value]()
      for (k <- row.keys) {
        val v1 = row.get(k).getOrElse(null)
        val v: Value = convertValue(v1)
        map1(k) = v
      }
      internalRecords.append(new Record(map1.toMap))
    }

    internalRecords
  }

}

