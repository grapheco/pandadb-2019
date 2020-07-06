package cn.pandadb.jraft

import java.io.File

import cn.pandadb.jraft.rpc.{ExecuteCypherRequestProcessor, ResultValueResponse}
import com.alipay.sofa.jraft.entity.PeerId
import com.alipay.sofa.jraft.option.NodeOptions
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory
import com.alipay.sofa.jraft.{Node, RaftGroupService}
import net.neoremind.kraps.rpc.netty.HippoRpcEnvFactory
import org.apache.commons.io.FileUtils
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

class PandaJraftServer(dataPath: String, groupId: String, serverId: PeerId,
                       nodeOptions: NodeOptions) {
  var raftGroupService: RaftGroupService = null
  var node : Node = null
  var pfsm: PandaJraftStateMachine = null
  var gdb: GraphDatabaseService = null
  var dataNodeService: PandaNodeService = null

  FileUtils.forceMkdir(new File(dataPath))
  var rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint)

  this.gdb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(dataPath + File.separator + "neo4j"))
  dataNodeService = new PandaNodeService(this.gdb)

  val pandaJraftService = new PandaJraftServiceImpl(this)
  rpcServer.registerProcessor(new ExecuteCypherRequestProcessor(pandaJraftService))

  this.pfsm = new PandaJraftStateMachine(dataNodeService)

  nodeOptions.setFsm(this.pfsm)
  nodeOptions.setLogUri(dataPath + File.separator + "log")
  nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta")
  nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot")
  this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer)

  def start(): Unit = {
    this.node = this.raftGroupService.start()
  }
  def getFsm: PandaJraftStateMachine = this.pfsm
  def getNode: Node = this.node
  def getRaftGroupService : RaftGroupService = this.raftGroupService
  def getDataNodeService : PandaNodeService = this.dataNodeService

  def redirect: ResultValueResponse = {
    val response = new ResultValueResponse(null, false, null, null)
    response.setSuccess(false)
    if (this.node != null) {
      val leader = this.node.getLeaderId
      if (leader != null) response.setRedirect(leader.toString)
    }
    response
  }

}
