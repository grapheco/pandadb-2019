package distributed

import cn.pandadb.tool.PNodeServerStarter

/**
  * Created by bluejoe on 2019/11/24.
  */
object PNodeServerStarterTest {
  def main(args: Array[String]) {
    val num = args(0)
    PNodeServerStarter.main(Array(s"./output/testdb/db${num}",
      s"./testdata/localnode${num}.conf"));
  }
}