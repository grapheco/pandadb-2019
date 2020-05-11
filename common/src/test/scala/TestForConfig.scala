
import org.junit.Test
import java.io.File

import cn.pandadb.configuration.{Config, SettingKeys}

class TestForConfig {
  @Test
  def test1(): Unit = {
    val config = new Config().withFile(Option(new File("testdata/test1.conf")))
    assert(config.getZKAddress().equals("127.0.0.1:2182") )
    assert(config.getPandaZKDir().equals("/pandadb/v0.0.3/") )
    assert(config.getListenHost().equals("127.0.0.1") )
    assert(config.getRpcPort().equals(52301) )
    assert(config.getRpcServerName().equals("pandadb-server2") )
    assert(config.getDataNodeEndpointName().equals("data-node-endpoint2") )
    assert(config.getLeaderNodeEndpointName().equals("leader-node-endpoint2") )
    assert(config.getLocalNeo4jDatabasePath().equals("/pandadb/db/graph2.db") )
  }

  @Test
  def test2(): Unit = {
    val settings = Map(
      SettingKeys.zkAddress->"127.0.0.1:2189",
      SettingKeys.zkPandaDBPath->"/pandadb/v3/",
      SettingKeys.rpcListenHost->"192.168.0.1",
      SettingKeys.rpcListenPort->"55555",
      SettingKeys.rpcServerName->"pandadb-sv",
      SettingKeys.dataNodeRpcEndpointName->"datanode",
      SettingKeys.leaderNodeRpcEndpointName->"leadernode",
      SettingKeys.localNeo4jDBPath->"graphdb.db")

    val config = new Config().withFile(Option(new File("testdata/test1.conf")))
      .withSettings(settings)
    assert(config.getZKAddress().equals(settings.get(SettingKeys.zkAddress).get))
    assert(config.getPandaZKDir().equals(settings.get(SettingKeys.zkPandaDBPath).get))
    assert(config.getListenHost().equals(settings.get(SettingKeys.rpcListenHost).get))
    assert(config.getRpcPort().equals(settings.get(SettingKeys.rpcListenPort).get.toInt))
    assert(config.getRpcServerName().equals(settings.get(SettingKeys.rpcServerName).get))
    assert(config.getDataNodeEndpointName().equals(settings.get(SettingKeys.dataNodeRpcEndpointName).get))
    assert(config.getLeaderNodeEndpointName().equals(settings.get(SettingKeys.leaderNodeRpcEndpointName).get))
    assert(config.getLocalNeo4jDatabasePath().equals(settings.get(SettingKeys.localNeo4jDBPath).get))
  }

}
