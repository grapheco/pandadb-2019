package cn.pandadb.server

import java.io._

import com.google.gson.Gson

import scala.io.Source

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

class JsonDataLogRW(logFile: File) extends DataLogWriter with DataLogReader {

  val logFIleIter: Iterator[String] = Source.fromFile(logFile).getLines()

  val _gson = new Gson()

  private var lastVersion: Int = {
    if (logFile.length() == 0) {
      -1
    } else {
      var _tempVersion = -1
      val _iter = Source.fromFile(logFile).getLines()
      _iter.foreach(line => {
        val _lineSerialNum = _gson.fromJson(line, new DataLogDetail(0, "").getClass).versionNum
        if(_lineSerialNum > _tempVersion) {
          _tempVersion = _lineSerialNum
        }
      })
      _tempVersion
    }
  }

  override def consume[T](consumer: DataLogDetail => T, sinceVersion: Int): Iterable[T] = {
    logFIleIter.toIterable.map(line => _gson.fromJson(line, new DataLogDetail(0, "").getClass))
      .filter(dataLogDetail => dataLogDetail.versionNum > sinceVersion)
      .map(consumer(_))
  }

  override def write(row: DataLogDetail): Unit = {
    lastVersion += 1
    val _fileAppender = new FileWriter(logFile, true)
    _fileAppender.append(s"${_gson.toJson(row)}\n");
    _fileAppender.flush()
    _fileAppender.close()
  }

  override def getLastVersion(): Int = {
    lastVersion
  }
}
