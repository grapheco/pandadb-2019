package org.neo4j.kernel.impl

import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrInputDocument
import org.neo4j.cypher.internal.runtime.interpreted.NodeFieldPredicate

import scala.collection.JavaConversions._

/**
  * Created by bluejoe on 2019/10/7.
  */
class InSolrPropertyNodeStore extends CustomPropertyNodeStore {
  var _solrClient: Option[CloudSolrClient] = None;

  override def deleteNodes(docsToBeDeleted: Iterable[Long]): Unit = {
    _solrClient.get.deleteById(docsToBeDeleted.map(_.toString).toList);
  }

  override def addNodes(docsToAdded: Iterable[CustomPropertyNode]): Unit = {
    _solrClient.get.add(docsToAdded.map { x =>
      val doc = new SolrInputDocument();
      x.fields.foreach(y => doc.addField(y._1, y._2));
      doc
    })
  }

  override def init(): Unit = {
    val zkUrl = ""
    val collectionName = "graiphdb"
    val _client = new CloudSolrClient(zkUrl);
    _client.setZkClientTimeout(30000);
    _client.setZkConnectTimeout(50000);
    _client.setDefaultCollection(collectionName);

    _solrClient = Some(_client);
  }

  override def filterNodes(expr: NodeFieldPredicate): Iterable[CustomPropertyNode] = {
    Array[CustomPropertyNode]()
  }

  override def updateNodes(docsToUpdated: Iterable[CustomPropertyNodeModification]): Unit = {

  }
}
