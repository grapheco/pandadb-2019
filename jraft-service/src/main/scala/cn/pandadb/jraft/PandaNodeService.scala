package cn.pandadb.jraft
import io.netty.handler.codec.ValueConverter
import org.neo4j.graphdb.{Direction, GraphDatabaseService, Label, RelationshipType, Result, Node => DbNode, Relationship => DbRelationship}
class PandaNodeService(localDatabase: GraphDatabaseService) {

  def runCypher(cypher: String): Result = {
    val tx = localDatabase.beginTx()
    val result = localDatabase.execute(cypher)
    //val internalRecords = result
    tx.success()
    tx.close()
    result
  }
}
