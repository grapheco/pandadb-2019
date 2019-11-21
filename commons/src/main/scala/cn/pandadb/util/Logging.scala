package cn.pandadb.util

/**
  * Created by bluejoe on 2019/10/9.
  */
import org.slf4j.LoggerFactory

/**
  * Created by bluejoe on 2019/5/24.
  */
trait Logging {
  val logger = LoggerFactory.getLogger(this.getClass);
}
