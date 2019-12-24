package distributed

import java.io.File

import cn.pandadb.server.PNodeServer
/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 8:59 2019/12/24
  * @Modified By:
  */

class LocalServerThread(num: Int) extends Runnable {
  override def run(): Unit = {
    PNodeServer.startServer(new File(s"../itest/output/testdb/db${num}"),
      new File(s"../itest/testdata/localnode${num}.conf"))
  }
}