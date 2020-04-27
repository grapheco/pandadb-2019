package cn.pandadb.neo4j.driver.values

object Types extends Enumeration {
  val ANY = Value(0, "any")
  val BOOLEAN = Value(1, "boolean")
  val BYTES = Value(2, "bytes")
  val STRING = Value(3, "string")
  val NUMBER = Value(4, "number")
  val INTEGER = Value(5, "integer")
  val FLOAT = Value(6, "float")
  val DATE = Value(7, "date")
  val TIME = Value(8, "time")
  val DATE_TIME = Value(9, "datetime")
  val LOCAL_TIME = Value(10, "local_time")
  val LOCAL_DATE_TIME = Value(11, "local_date_time")
  val DURATION = Value(12, "duration")
  val POINT = Value(13, "point")

  val LIST = Value(14, "list")
  val MAP = Value(15, "map")

  val NODE = Value(16, "node")
  val RELATIONSHIP = Value(17, "relationship")
  val PATH = Value(18, "path")
  val NULL = Value(19, "null")

}
