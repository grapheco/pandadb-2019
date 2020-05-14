package cn.pandadb.index

import java.util.ServiceLoader
import scala.collection.JavaConverters._

import cn.pandadb.configuration.Config
import cn.pandadb.index.{IndexServiceFactory, IndexService}
import org.junit.Test

class TestsForIndexService {

  @Test
  def test1(): Unit = {
    val serviceLoaders = ServiceLoader.load(classOf[IndexServiceFactory]).asScala
    assert(serviceLoaders.size > 0)
    val indexService = serviceLoaders.iterator.next().create(new Config)
    assert (indexService.addNode(0))
  }
}
