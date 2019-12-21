package cn.pandadb.network

import java.io.{File, FileInputStream}
import java.util.Properties
import cn.pandadb.util.ConfigUtils._

import cn.pandadb.util.{InstanceContext}

object ZKConstants {
  val localNodeAddress = InstanceContext.getRequiredValueAsString(s"localNodeAddress")
  val zkServerAddress = InstanceContext.getRequiredValueAsString(s"zkServerAddress")
}
