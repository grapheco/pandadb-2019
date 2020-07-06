package cn.pandadb.jraft

trait PandaJraftService {
  def executeWCypher(cypher: String, closure: PandadbJraftClosure)
  def executeRCypher(cypher: String, closure: PandadbJraftClosure)
}
