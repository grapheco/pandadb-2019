package cn.pandadb.jraft

import java.util.concurrent.Executor

import cn.pandadb.jraft.rpc.ExecuteCypherRequest
import com.alipay.sofa.jraft.RouteTable
import com.alipay.sofa.jraft.conf.Configuration
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.CliOptions
import com.alipay.sofa.jraft.rpc.InvokeCallback
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl
import org.neo4j.graphdb.Result
// scalastyle:off println
class PandaJraftClient(groupId: String, confStr: String) {
  val conf = new Configuration
  if (!conf.parse(confStr)) throw new IllegalArgumentException("Fail to parse conf:" + confStr)

  RouteTable.getInstance.updateConfiguration(groupId, conf)

  val cliClientService = new CliClientServiceImpl
  cliClientService.init(new CliOptions)

  if (!RouteTable.getInstance.refreshLeader(cliClientService, groupId, 1000).isOk) throw new IllegalStateException("Refresh leader failed")

  val leader: PeerId = RouteTable.getInstance.selectLeader(groupId)

  System.out.println("leader:" + leader)
  def runCypher(cypher: String): Result = {
    val request = new ExecuteCypherRequest
    var res: Result = null
    request.setCypher(cypher)
    cliClientService.getRpcClient.invokeAsync(leader.getEndpoint, request, new InvokeCallback() {
      override def complete(result: Any, err: Throwable): Unit = {
        res = result.asInstanceOf[Result]
        if (err == null) {
          System.out.println("incrementAndGet result:" + result)
        }
        else {
          err.printStackTrace()
        }
      }

      override

      def executor: Executor = null
    }, 5000)
    res
  }
}
