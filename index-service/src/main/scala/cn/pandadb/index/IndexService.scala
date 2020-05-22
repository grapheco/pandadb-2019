package cn.pandadb.index

import cn.pandadb.configuration.Config
import cn.pandadb.server.modules.LifecycleServerModule

trait IndexServiceFactory {
  def create(config: Config): IndexService
}

trait IndexService extends LifecycleServerModule with NodeIndexReader with NodeIndexWriter {
}

trait NodeIndexWriter {
  def deleteNode(nodeId: Long);

  def addNode(nodeId: Long): Boolean;

  def addProperty(nodeId: Long, key: String, value: Any): Unit;

  def removeProperty(nodeId: Long, key: String);

  def updateProperty(nodeId: Long, key: String, value: Any): Unit;

  def addLabel(nodeId: Long, label: String): Unit;

  def removeLabel(nodeId: Long, label: String): Unit;
}

trait NodeIndexReader {
//  def filterNodes(expr: NFPredicate): Iterable[Long];

  def getNodesByLabel(label: String): Iterable[Long];

  def getNodesByProperties(props: Map[String, Any]);

//  def getNodeBylabelAndFilter(label: String, expr: NFPredicate): Iterable[Long];

}

