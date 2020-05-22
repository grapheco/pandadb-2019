package cn.pandadb.util

object PandaReplyMessage extends Enumeration {
  val SUCCESS = Value(1)
  val FAILED = Value(0)

  val LEAD_NODE_SUCCESS = Value(2)
  val LEAD_NODE_FAILED = Value(3)
}
