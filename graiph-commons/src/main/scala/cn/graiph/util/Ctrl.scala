package cn.graiph.util

/**
  * Created by bluejoe on 2019/11/14.
  */
object Ctrl extends Logging {
  def run[T](comment: String)(body: => T): T = {
    logger.debug(comment)
    body
  }
}
