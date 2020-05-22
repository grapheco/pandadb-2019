
import java.io.File

import cn.pandadb.blob.{Blob, BlobEntry, BlobId, MimeType}
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseFactory

class TestForBlob {
  @Test
  def testCreateBlobValue(): Unit = {
    val dbFile = new File("output/graph.db")
    if (dbFile.exists()) {
      dbFile.delete()
    }
    var db: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dbFile).newGraphDatabase()

    val tx = db.beginTx()
    val n1 = db.createNode()
    val b1: BlobEntry = Blob.makeEntry(new BlobId(1, 1), 10,
      MimeType.fromText("application/octet-stream"))
    n1.setProperty("b1", b1)
    assert(n1.getProperty("b1").equals(b1))
    tx.success()
    tx.close()

    val tx2 = db.beginTx()
    val n2 = db.getNodeById(n1.getId)
    val b2 = n2.getProperty("b1").asInstanceOf[BlobEntry]
    assert(b2.compareTo(b1) == 0)
    tx2.close()

  }
}
