package distributed

import cn.pandadb.tool.PNodeServerStarter

/**
  * Created by bluejoe on 2019/11/24.
  */
object PNodeServerStarterTest {
  def main(args: Array[String]) {
    val num = 1
    PNodeServerStarter.main(Array(s"./itest/output/testdb/db${num}",
      s"./itest/testdata/gnode${num}.conf"));
  }
}
