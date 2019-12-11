package performance

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 16:33 2019/12/11
  * @Modified By:
  */

class ResultMap(cypher: String, timeList: Array[Long]) {

  private val _resultMap = mutable.Map[String, Any]()

  def put[T](key: String, value: T): T = {
    _resultMap(key) = value
    value
  };

  def getResultMap: java.util.Map[String, Any] = {
    this._putCypher()
    this._putSessionCreationTime
    this._putTxCreationTime
    this._putExecutionTime
    this._putTxCloseTime
    this._putSessionCloseTime
    this._putRespTime
    _resultMap.toMap.asJava
  }

  private def _putCypher(): Unit = {
    this.put("cypher", cypher)
  }
  private def _putSessionCreationTime: Unit = {
    this.put("sessionCreation", (timeList(1) - timeList(0)).toInt)
  }
  private def _putTxCreationTime: Unit = {
    this.put("txCreation", (timeList(2) - timeList(1)).toInt)
  }
  private def _putExecutionTime: Unit = {
    this.put("executionTime", (timeList(3) - timeList(2)).toInt)
  }
  private def _putTxCloseTime: Unit = {
    this.put("txClose", (timeList(4) - timeList(3)).toInt)
  }
  private def _putSessionCloseTime: Unit = {
    this.put("sessionClose", (timeList(5) - timeList(4)).toInt)
  }
  private def _putRespTime: Unit = {
    this.put("totalRespTime", (timeList(5) - timeList(0)).toInt)
  }
}
