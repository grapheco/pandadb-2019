package cn.pandadb.driver.result

import scala.collection.mutable.ArrayBuffer

trait Result extends Iterator[Record] {
}

class InternalRecords extends Serializable {
  val records = new ArrayBuffer[Record]()

  def append(record: Record): Unit = records.append(record)

  def getRecords(): Iterable[Record] = records

  override def toString: String = records.toString().toString
}
