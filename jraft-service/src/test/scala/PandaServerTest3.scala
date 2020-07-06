import cn.pandadb.jraft.PandaServerTool

object PandaServerTest3 {

  def main(args: Array[String]): Unit = {
    //if (args.length!= 4) return
    val dataPath = "pandaServer/temp3"
    val groupId = "pandaserver"
    val serverIdStr = "127.0.0.1:6063"
    val initConfStr = "127.0.0.1:6061,127.0.0.1:6062,127.0.0.1:6063"
    new PandaServerTool(dataPath, groupId, serverIdStr, initConfStr).start()
  }
}
