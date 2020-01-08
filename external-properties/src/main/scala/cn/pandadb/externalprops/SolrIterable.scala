package cn.pandadb.externalprops

import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.common.SolrDocument
import org.apache.log4j.Logger
import org.apache.solr.client.solrj.SolrQuery.ORDER
import org.apache.solr.common.params.CursorMarkParams

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.bufferAsJavaList
import scala.collection.JavaConversions.mapAsJavaMap
import scala.collection.JavaConversions.seqAsJavaList
import scala.collection.immutable.Map
import scala.collection.mutable.ArrayBuffer

class SolrQueryResults(_solrClient: CloudSolrClient, solrQuery: SolrQuery, pageSize: Int = 20) {

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

  def iterator2(): SolrQueryResultsCursorIterator = new SolrQueryResultsCursorIterator(_solrClient, solrQuery, pageSize)
}

class SolrQueryResultsCursorIterator(_solrClient: CloudSolrClient, solrQuery: SolrQuery, pageSize: Int = 20)
  extends Iterator[NodeWithProperties] {

  var startOfCurrentPage = 0;
  var rowIteratorWithinCurrentPage: java.util.Iterator[NodeWithProperties] = null;
  var isFinished = false;
  val mySolrQuery = solrQuery.getCopy();
  var cursorMark = CursorMarkParams.CURSOR_MARK_START
  var nextCursorMark: String = null
  private var currentData : Iterable[NodeWithProperties] = _
  mySolrQuery.setRows(pageSize)
  mySolrQuery.setSort(SolrUtil.idName, ORDER.asc)
  readNextPage();

  def doc2Node(doc : SolrDocument): NodeWithProperties = {
    SolrUtil.solrDoc2nodeWithProperties(doc)
  }

  def readNextPage(): Boolean = {

    mySolrQuery.set(CursorMarkParams.CURSOR_MARK_PARAM, cursorMark)
    val rsp = _solrClient.query(mySolrQuery)
    nextCursorMark = rsp.getNextCursorMark
    val docs = rsp.getResults()
    val rows = docs.map {doc2Node}
    currentData = null
    currentData = rows
    rowIteratorWithinCurrentPage = rows.iterator()
    if (cursorMark.equals(nextCursorMark)) {
      isFinished = true
      false
    }
    else {
      cursorMark = nextCursorMark
      true
    }

  }

  def hasNext(): Boolean = {
    rowIteratorWithinCurrentPage.hasNext() || readNextPage()
  }

  def next(): NodeWithProperties = {

    rowIteratorWithinCurrentPage.next()

  }

  def getCurrentData(): Iterable[NodeWithProperties] = {
    this.currentData
  }

}

class SolrQueryResultsIterator(_solrClient: CloudSolrClient, solrQuery: SolrQuery, pageSize: Int = 20)
  extends Iterator[NodeWithProperties] {

  var startOfCurrentPage = 0;
  var rowIteratorWithinCurrentPage: java.util.Iterator[NodeWithProperties] = null;
  var totalCountOfRows = -1L;
  val mySolrQuery = solrQuery.getCopy();
  var done = true
  private var currentData : Iterable[NodeWithProperties] = _
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