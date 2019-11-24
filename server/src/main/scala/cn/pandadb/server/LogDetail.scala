package cn.pandadb.server

import java.io.File

/**
  * Created by bluejoe on 2019/11/24.
  */
case class LogDetail(version: Int, command: String) {

}

trait LogWriter {
  def write(row: LogDetail): Unit;
}

trait LogReader {
  def consume[T](consumer: (LogDetail) => T, sinceVersion: Int = -1): Iterable[T];
}

abstract class JsonLogStore(logFile: File) extends LogWriter with LogReader {

}