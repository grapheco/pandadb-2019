package cn.pandadb.jraft

import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.NodeOptions

object PandaServerTool {
  def main(args: Array[String]): Unit = {
    //if (args.length!= 4) return
    val dataPath = "pandaServer/temp"
    val groupId = "pandaserver"
    val serverIdStr = "127.0.0.1:6061"
    val initConfStr = "127.0.0.1:6061,127.0.0.1:6062,127.0.0.1:6063"
    new PandaServerTool(dataPath, groupId, serverIdStr, initConfStr).start()
  }
}
class PandaServerTool(dataPath: String, groupId: String, serverIdStr: String,
                      initConfStr: String) {
  val nodeOptions = new NodeOptions
  nodeOptions.setElectionTimeoutMs(1000)
  nodeOptions.setDisableCli(false)
  nodeOptions.setSnapshotIntervalSecs(300000)
  val serverId = new PeerId()

  if(!(serverId.parse(serverIdStr))) {
    throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr)
  }
  final val initConf = new Configuration();
  if (!initConf.parse(initConfStr)) throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr)
  nodeOptions.setInitialConf(initConf);
  val server = new PandaJraftServer(dataPath, groupId, serverId, nodeOptions)
  def start(): Unit = {
    server.start()
  }
  //System.out.println("Started counter server at port:" + server.getNode.getNodeId.getPeerId.getPort)

}
