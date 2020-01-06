package cn.pandadb.server

import java.io._

import com.google.gson.Gson

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 13:10 2019/11/30
  * @Modified By:
  */

case class DataLogDetail(val versionNum: Int, val command: String) {

}

trait DataLogWriter {
  def write(row: DataLogDetail): Unit;
  def getLastVersion(): Int;
}

trait DataLogReader {
  def consume[T](consumer: (DataLogDetail) => T, sinceVersion: Int = -1): Iterable[T];
  def getLastVersion(): Int;
}

class DataLog(arrayBuffer: ArrayBuffer[DataLogDetail]) {
  val dataLog: Array[DataLogDetail] = arrayBuffer.toArray
}

class JsonDataLogRW(logFile: File) extends DataLogWriter with DataLogReader {

  val _gson = new Gson()

  val _bufferReader = new BufferedReader(new InputStreamReader(new FileInputStream(logFile)))

  // should not has the dataLog.
  var dataLog: ArrayBuffer[DataLogDetail] = {
    if (logFile.length() == 0) {
      new ArrayBuffer[DataLogDetail]()
    } else {
      val _dalaLog = new ArrayBuffer[DataLogDetail]()
      var line = _bufferReader.readLine()
      while(line!=null) {
        _dalaLog.append(_gson.fromJson(line, DataLogDetail.getClass))
        line = _bufferReader.readLine()
      }
      _dalaLog
    }
  }

  override def consume[T](consumer: DataLogDetail => T, sinceVersion: Int): Iterable[T] = {
    dataLog.toStream.filter(_.versionNum > sinceVersion)
      .map(consumer(_))
  }

  override def write(row: DataLogDetail): Unit = {
    dataLog.append(row)
    val _fileAppender = new FileWriter(logFile, true)
    _fileAppender.append(s"${_gson.toJson(row)}\n");
    _fileAppender.flush()
    _fileAppender.close()
  }

//  override def write(row: DataLogDetail): Unit = {
//    dataLog.append(row)
//    val fileWriter = new FileWriter(logFile)
//    val logStr = gson.toJson(new DataLog(dataLog))
//    fileWriter.write(logStr)
//    fileWriter.flush();
//    fileWriter.close();
//  }

  override def getLastVersion(): Int = {
    if (dataLog.length == 0) {
      -1
    } else {
      dataLog.last.versionNum
    }
  }
}
