package cn.graiph.cnode

trait CosistencyLock {
// Pre Write
  def lockRead()
  def lockWrite()
}


