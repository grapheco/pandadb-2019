package cn.pandadb.blob.storage

import java.io.FileInputStream

import cn.pandadb.blob.MimeType
import cn.pandadb.blob.storage.impl.RegionfsBlobValueStorage
import cn.pandadb.configuration.Config
import org.junit.Test

class TestsForRegionfsBlobValueStorage {
  val config1 = new Config() {
    override def getRegionfsZkAddress(): String = {"10.0.82.220:2181"}
  }

  @Test
  def test1(): Unit = {
    val storage = new RegionfsBlobValueStorage(config1)
    val ins = new FileInputStream("testdata/test.txt")
    val contentLength = ins.available()
    val blobEntry = storage.save(10, MimeType.fromCode(1), ins)
    val blob = storage.load(blobEntry.id).get
    assert(blob.toString.equals(blobEntry.toString))
    assert(blob.toBytes().length == contentLength)
  }

}
