package cn.pandadb.blob

import java.io.InputStream

/**
  * Created by bluejoe on 2019/11/10.
  */
trait InputStreamSource {
  /**
    * note close input stream after consuming
    */
  def offerStream[T](consume: (InputStream) => T): T;
}
