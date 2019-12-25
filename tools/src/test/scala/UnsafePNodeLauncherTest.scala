import org.junit.Test
import sys.process._

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 20:04 2019/12/25
  * @Modified By:
  */
class UnsafePNodeLauncherTest {

  @Test
  def test1(): Unit = {
    val startCmd = "cmd.exe /c mvn exec:java -Dexec.mainClass='cn.pandadb.tool.UnsafePNodeLauncher' -Dexec.args=0" !!;
    Thread.sleep(999999)
  }
}
