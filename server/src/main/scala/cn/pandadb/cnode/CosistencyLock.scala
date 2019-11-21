package cn.pandadb.cnode

trait CosistencyLock {
// Pre Write
  def lockRead()
  def lockWrite()
}


