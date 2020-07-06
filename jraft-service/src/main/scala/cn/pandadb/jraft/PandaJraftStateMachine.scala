package cn.pandadb.jraft

import java.util.concurrent.atomic.AtomicLong

import com.alipay.remoting.serialization.SerializerManager
import com.alipay.sofa.jraft
import com.alipay.sofa.jraft.Closure
import com.alipay.sofa.jraft.core.StateMachineAdapter
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter
import com.alipay.sofa.jraft.Status
import org.neo4j.graphdb.Result
// scalastyle:off println
class PandaJraftStateMachine(dataNodeServiceImpl: PandaNodeService) extends StateMachineAdapter{

  var leaderTerm = new AtomicLong(-1)
  def runCypher(cypher: String): Result = {
    dataNodeServiceImpl.runCypher(cypher)
  }
  def isLeader: Boolean = this.leaderTerm.get() > 0
  override def onApply(iter: jraft.Iterator): Unit = {
    while (iter.hasNext) {
      var closure: PandadbJraftClosure = null
      var cypher: String = null
      if (iter.done() != null) {
        closure = iter.done().asInstanceOf[PandadbJraftClosure]
        cypher = closure.getCypher
      }
      else {
        val data = iter.getData
        cypher = SerializerManager.getSerializer(SerializerManager.Hessian2).deserialize(data.array, "String")
      }
      System.out.println("incrementAndGet result:" + cypher)
      if (cypher != null) {
        val res = runCypher(cypher)
        if (closure!=null) {
          closure.success(res)
          closure.run(Status.OK())
        }
      }
      iter.next()
    }
  }

  override def onLeaderStart(term: Long): Unit = {
    this.leaderTerm.set(term)
    super.onLeaderStart(term)
  }

  override def onLeaderStop(status: jraft.Status): Unit = {
    this.leaderTerm.set(-1)
    super.onLeaderStop(status)
  }
}
