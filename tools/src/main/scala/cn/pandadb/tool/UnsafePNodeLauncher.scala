package cn.pandadb.tool

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 22:27 2019/12/25
  * @Modified By:
  */
object UnsafePNodeLauncher {
  def main(args: Array[String]): Unit = {
    val num = args(0)
    PNodeServerStarter.main(Array(s"./itest/output/testdb/db${num}",
      s"./itest/testdata/localnode${num}.conf"))
  }
}
