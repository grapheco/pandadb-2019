
import org.junit.Test
import java.io.File
import cn.pandadb.configuration.Config

class TestForConfig {
  @Test
  def test1(): Unit = {
    val config = new Config().withFile(Option(new File("testdata/test1.conf")))
    assert(config.getZKAddress().equals("127.0.0.1:2182") )
  }

}
