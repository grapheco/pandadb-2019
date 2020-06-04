package cn.pandadb.index.impl

import cn.pandadb.configuration.Config
import cn.pandadb.index.{IndexService, IndexServiceFactory}

object BambooIndexServiceFactory extends IndexServiceFactory {
   override def create(config: Config): BambooIndexService = {
    new BambooIndexService(config)
  }
}

class BambooIndexService(config: Config) extends IndexService {
  val logger = config.getLogger(this.getClass)

//  val client = new Client

  override def init(): Unit = {
    logger.info(this.getClass + ": init")
  }

  override def start(): Unit = {
    logger.info(this.getClass + ": start")
  }

  override def stop(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def shutdown(): Unit = {
    logger.info(this.getClass + ": stop")
  }

  override def deleteNode(nodeId: Long): Unit = {
    logger.info(this.getClass + ": deleteNode")
  }

  override def addNode(nodeId: Long): Boolean = {
    logger.info(this.getClass + ": addNode")
    true
  }

  override def addProperty(nodeId: Long, key: String, value: Any): Unit = {
    logger.info(this.getClass + ": addProperty")
  }

  override def removeProperty(nodeId: Long, key: String): Unit = {
    logger.info(this.getClass + ": removeProperty")
  }

  override def updateProperty(nodeId: Long, key: String, value: Any): Unit = {
    logger.info(this.getClass + ": updateProperty")
  }

  override def addLabel(nodeId: Long, label: String): Unit = {
    logger.info(this.getClass + ": addLabel")
  }

  override def removeLabel(nodeId: Long, label: String): Unit = {
    logger.info(this.getClass + ": removeLabel")
  }

  override def getNodesByLabel(label: String): Iterable[Long] = {
    logger.info(this.getClass + ": getNodesByLabel")
    null
  }

  override def getNodesByProperties(props: Map[String, Any]): Unit = {
    logger.info(this.getClass + ": getNodesByProperties")
  }
}
