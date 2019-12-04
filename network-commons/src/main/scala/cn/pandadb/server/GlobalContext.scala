package cn.pandadb.server

import java.io.File

import cn.pandadb.network.ClusterClient
import cn.pandadb.util.ContextMap

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 14:42 2019/12/4
  * @Modified By:
  */
object GlobalContext extends ContextMap {

  def bindStoreDir(storeDir: File): Unit = {
    this.put[File]("pnode.store.dir", storeDir)
  }

  def getStoreDir: File = this.get[File]("pnode.store.dir")
}
