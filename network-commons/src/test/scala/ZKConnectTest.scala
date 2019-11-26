import java.io.File

import cn.pandadb.context.Neo4jConfigUtils
import cn.pandadb.network.ZKConstants
import cn.pandadb.util.ConfigUtils
import org.neo4j.kernel.configuration.Config

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created in 11:07 2019/11/26
  * @Modified By:
  */
class ZKConnectTest {

  val configFile = new File(this.getClass.getClassLoader.getResource("test_pnode0.conf").getPath)
  val neo4jConfig = Config.builder().withFile(configFile).build()
  val pandaConfig = Neo4jConfigUtils.neo4jConfig2Config(neo4jConfig)
  val pandaConfigEX = ConfigUtils.config2Ex(pandaConfig)
  val zkContestants = new ZKConstants(pandaConfigEX)


}
