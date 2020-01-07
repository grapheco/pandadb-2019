package cn.pandadb.externalprops

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrDocument
import org.apache.log4j.Logger

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

class SolrIterable {

}

class SolrQueryResults(_solrClient: CloudSolrClient, solrQuery: SolrQuery, pageSize: Int = 20) extends java.lang.Iterable[NodeWithProperties] {

  def iterator(): SolrQueryResultsIterator = new SolrQueryResultsIterator(_solrClient, solrQuery, pageSize)

  def getAllResults(): Iterable[NodeWithProperties] = {
    val nodeArray = ArrayBuffer[NodeWithProperties]()
    solrQuery.setRows(SolrUtil.maxRows)
    val res = _solrClient.query(solrQuery).getResults
    res.foreach(
      x => {
        nodeArray += SolrUtil.solrDoc2nodeWithProperties(x)
      }
    )
    nodeArray
  }
}

class SolrQueryResultsIterator(_solrClient: CloudSolrClient, solrQuery: SolrQuery, pageSize: Int = 20)
  extends java.util.Iterator[NodeWithProperties] {

  var startOfCurrentPage = 0;
  var rowIteratorWithinCurrentPage: java.util.Iterator[NodeWithProperties] = null;
  var totalCountOfRows = -1L;
  val mySolrQuery = solrQuery.getCopy();
  var currentData : Iterable[NodeWithProperties] = _
  readNextPage();

  def doc2Node(doc : SolrDocument): NodeWithProperties = {
    SolrUtil.solrDoc2nodeWithProperties(doc)
  }

  def readNextPage(): Boolean = {

    if (totalCountOfRows < 0 || startOfCurrentPage < totalCountOfRows) {
      mySolrQuery.set("start", startOfCurrentPage);
      mySolrQuery.set("rows", pageSize);
      startOfCurrentPage += pageSize;
      //logger.debug(s"executing solr query: $mySolrQuery");
      val rsp = _solrClient.query(mySolrQuery);
      val docs = rsp.getResults();
      totalCountOfRows = docs.getNumFound();
      //logger.debug(s"numFound: $totalCountOfRows");
      val rows = docs.map {doc2Node};
      currentData = null
      currentData = rows
      rowIteratorWithinCurrentPage = rows.iterator();
      true;
    }
    else {
      false;
    }
  }

  def hasNext(): Boolean = {
    rowIteratorWithinCurrentPage.hasNext() || startOfCurrentPage < totalCountOfRows
  }

  def next(): NodeWithProperties = {

    if (!rowIteratorWithinCurrentPage.hasNext()) {

      if (!readNextPage()) throw new NoSuchElementException();

    }
    rowIteratorWithinCurrentPage.next()

  }

  def getCurrentData(): Iterable[NodeWithProperties] = {
    this.currentData
  }

}