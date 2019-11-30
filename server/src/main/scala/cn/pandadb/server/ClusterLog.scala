package cn.pandadb.server

import java.io.File

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 13:10 2019/11/30
  * @Modified By:
  */
case class DataLogDetail(version: Int, command: String) {

}

trait LogWriter {
  def write(row: DataLogDetail): Unit;
}

trait LogReader {
  def consume[T](consumer: (DataLogDetail) => T, sinceVersion: Int = -1): Iterable[T];
}

abstract class JsonLogStore(logFile: File) extends LogWriter with LogReader {

}
