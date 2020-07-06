package cn.pandadb.jraft

import java.nio.ByteBuffer
import java.util.concurrent.Executor

import com.alipay.sofa.jraft.rhea.StoreEngineHelper
import com.alipay.sofa.jraft.rhea.options.StoreEngineOptions
import cn.pandadb.jraft.rpc.ResultValueResponse
import com.alipay.remoting.serialization.SerializerManager
import com.alipay.sofa.jraft.Status
import com.alipay.sofa.jraft.entity.Task
import com.alipay.sofa.jraft.error.RaftError
import org.neo4j.graphdb.Result
// scalastyle:off println
class PandaJraftServiceImpl(pandaJraftServer: PandaJraftServer) extends PandaJraftService {

  val readIndexExecutor = createReadIndexExecutor
  //val dataNodeService = new DataNodeServiceImpl()

  def createReadIndexExecutor: Executor = {
    val opts = new StoreEngineOptions
    StoreEngineHelper.createReadIndexExecutor(opts.getReadIndexCoreThreads)
  }
  def isLeader: Boolean = this.pandaJraftServer.getFsm.isLeader
  def getRedirect: String = this.pandaJraftServer.redirect.getRedirect
  override def executeWCypher(cypher: String, closure: PandadbJraftClosure): Unit = {
    System.out.println("incrementAndGet result:" + cypher)
    if (!isLeader) handlerNotLeaderError(closure)
    else {
      val task = new Task()
      closure.setCypher(cypher)
      task.setData(ByteBuffer.wrap(SerializerManager.getSerializer(SerializerManager.Hessian2).serialize(cypher)))
      task.setDone(closure)
      this.pandaJraftServer.getNode.apply(task)
    }
  }

  def runCypher(cypher: String): Result = {
    this.pandaJraftServer.getFsm.runCypher(cypher)
  }
  override def executeRCypher(cypher: String, closure: PandadbJraftClosure): Unit = {
    System.out.println("incrementAndGet result:" + cypher)
    val res = runCypher(cypher)
    closure.setValueResponse(new ResultValueResponse(res, true, null, null))
    closure.run(Status.OK())
  }
  def handlerNotLeaderError(closure: PandadbJraftClosure): Unit = {
    closure.failure("not leader", getRedirect)
    closure.run(new Status(RaftError.EPERM, "not Leader"))
  }
}
