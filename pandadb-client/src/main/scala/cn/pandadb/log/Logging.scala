package cn.pandadb.log

import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  def getLogger(clazz: Class[_]): Logger = {
    LoggerFactory.getLogger(clazz)
  }

}
