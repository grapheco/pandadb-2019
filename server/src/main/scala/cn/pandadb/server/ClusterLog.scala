package cn.pandadb.server

import java.io.{File, FileReader, FileWriter}

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
}

class DataLog(arrayBuffer: ArrayBuffer[DataLogDetail]) {
  val dataLog: Array[DataLogDetail] = arrayBuffer.toArray
}

class JsonDataLogRW(logFile: File) extends DataLogWriter with DataLogReader {
  val gson = new Gson()

  var dataLog: ArrayBuffer[DataLogDetail] = {
    if (logFile.length() == 0) {
      new ArrayBuffer[DataLogDetail]()
    } else {
      val array = gson.fromJson(new FileReader(logFile), new DataLog(new ArrayBuffer[DataLogDetail]()).getClass)
      new ArrayBuffer[DataLogDetail]() ++= array.dataLog
    }
  }

  override def consume[T](consumer: DataLogDetail => T, sinceVersion: Int): Iterable[T] = {
    dataLog.toStream.filter(_.versionNum > sinceVersion)
      .map(consumer(_))
  }

  override def write(row: DataLogDetail): Unit = {
    dataLog.append(row)
    val fileWriter = new FileWriter(logFile)
    val logStr = gson.toJson(new DataLog(dataLog))
    fileWriter.write(logStr)
    fileWriter.flush();
    fileWriter.close();
  }

  override def getLastVersion(): Int = {
    if (dataLog.length == 0) {
      -1
    } else {
      dataLog.last.versionNum
    }
  }
}
