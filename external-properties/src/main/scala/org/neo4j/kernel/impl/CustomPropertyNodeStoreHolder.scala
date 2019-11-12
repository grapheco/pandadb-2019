package org.neo4j.kernel.impl

import cn.graiph.util.Logging
import org.neo4j.cypher.internal.runtime.interpreted.NFPredicate

/**
  * Created by bluejoe on 2019/10/7.
  */
object CustomPropertyNodeStoreHolder {
  var _propertyNodeStore: Option[CustomPropertyNodeStore] = None;

  def hold(propertyNodeStore: CustomPropertyNodeStore): Unit = {
    _propertyNodeStore = Some(propertyNodeStore);
    propertyNodeStore.init();
  }

  def isDefined = _propertyNodeStore.isDefined;
  def get: CustomPropertyNodeStore = _propertyNodeStore.get;
}

object Settings {
  var _hookEnabled = false;
  var _patternMatchFirst = true;
}

class LoggingPropertiesStore(source: CustomPropertyNodeStore) extends CustomPropertyNodeStore with Logging {
  override def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    logger.debug(s"deleteNodes: $docsToBeDeleted")
    source.deleteNodes(docsToBeDeleted)
  }

  override def init(): Unit = {
    logger.debug(s"init()")
    source.init()
  }

  override def addNodes(docsToAdded: Iterable[CustomPropertyNode]): Unit = {
    logger.debug(s"addNodes:$docsToAdded")
    source.addNodes(docsToAdded)
  }

  override def filterNodes(expr: NFPredicate): Iterable[CustomPropertyNode] = {
    val ir = source.filterNodes(expr)
    logger.debug(s"filterNodes(expr=$expr): $ir")
    ir;
  }

  override def updateNodes(docsToUpdated: Iterable[CustomPropertyNodeModification]): Unit = {
    logger.debug(s"docsToUpdated: $docsToUpdated")
    source.updateNodes(docsToUpdated)
  }

  override def getNodesByLabel(label: String): Iterable[CustomPropertyNode] = {
    val res = source.getNodesByLabel(label)
    logger.debug(s"getNodesByLabel: result=> $res")
    res
  }
}
