package cn.pandadb.client

import cn.pandadb.blob.{BlobEntry, MimeType}
import cn.pandadb.driver.values.BlobEntryValue
import org.junit.Test

class Tests {

  val client = new PandaDBClient("127.0.0.1:2181")

  @Test
  def test1(): Unit = {
    val res1 = client.createNode(Array("Person"), Map("name"->"t1", "age"->10))
    val res2 = client.createNode(Array("Person", "boy"), Map("name"->"t2", "age"->15, "arr"->Array(1, 2, 3)))
    val res3 = client.createNode(Array("Person", "boy"), Map("name"->"t3", "bb"->15, "arr"->Array("1", "2")))
    val res4 = client.runCypher("match (n:Person) return n")
    assert(res4.records.size == 3)
  }

  @Test
  def test2(): Unit = {
    val blobEntry: BlobEntry = client.createBlobFromFile(10, MimeType.fromText("application/octet-stream"), null)
    val res1 = client.createNode(Array("Actor"), Map("name"->"t1", "blob"->blobEntry))
    val nodes1 = client.getNodesByLabel("Actor")
    assert(nodes1.size >= 1)
    for (n <- nodes1) {
      assert(n.props("blob").isInstanceOf[BlobEntryValue])
      assert(n.props("blob").asBlobEntry().toString.equals(blobEntry.toString))
    }
  }

}
