import java.io.File

import cn.pandadb.server.{DataLogDetail, JsonDataLog}
import com.google.gson.Gson
import org.junit.Test
/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 19:21 2019/12/1
  * @Modified By:
  */
class ClusterLogTest {

  val dataLogDetail = new DataLogDetail(100, "Match(n) Return(n);")
  val gsonItem = new Gson().toJson(dataLogDetail)

  val logFilePath: String = "./datalog.json"
  val logFile = new File(logFilePath)

  @Test
  def test1(): Unit = {
    val jsonDataLog = new JsonDataLog(logFile)
    jsonDataLog.write(new DataLogDetail(100, "Match(n), return n;"))
    jsonDataLog.write(new DataLogDetail(200, "qwe"))
  }

}