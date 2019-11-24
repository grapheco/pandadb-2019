package cn.pandadb.util

/**
  * Created by bluejoe on 2019/11/14.
  */
object Ctrl {
  def run[T](comment: String)(body: => T): T = {
    body
  }
}
