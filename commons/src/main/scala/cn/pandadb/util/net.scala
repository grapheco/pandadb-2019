package cn.pandadb.util

/**
  * Created by bluejoe on 2019/11/21.
  */
case class NodeAddress(host: String, port: Int) {
}

object NodeAddress {
  def fromString(url: String, seperator: String = ":"): NodeAddress = {
    val pair = url.split(seperator)
    NodeAddress(pair(0), pair(1).toInt)
  }
}