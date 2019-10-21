package cn.aipm.service

import java.io.{File, InputStream}
import java.nio.charset.Charset

import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.conn.HttpHostConnectException
import org.apache.http.entity.ContentType
import org.apache.http.entity.mime.{HttpMultipartMode, MultipartEntityBuilder}
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpEntity, HttpStatus}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer

object WebUtils {

  def doGet(reqUrl: String, contents: Map[String, Any]): String = {
    var resStr = ""
    try {
      val client = HttpClients.createDefault()
      val httpGet: HttpGet = new HttpGet(reqUrl)

      val response = client.execute(httpGet)

      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode == HttpStatus.SC_OK) {
        val resEntity: HttpEntity = response.getEntity
        resStr = EntityUtils.toString(resEntity, "UTF-8")
      }
    } catch {
      case e: HttpHostConnectException =>
        throw new AipmServiceException(s"Failed connect to ${e.getHost} ")
      case e: Throwable => throw new AipmServiceException(e.getMessage)
    }

    resStr
  }


  def doPost(reqUrl: String, strContents: Map[String, String] = Map(), fileContents: Map[String, File] = Map(),
             inStreamContents: Map[String, InputStream] = Map()): String = {
    var resStr = ""
    try {
      val httpClient = HttpClients.createDefault()
      val httpPost = new HttpPost(reqUrl)
      val mEntityBuilder = MultipartEntityBuilder.create()
      mEntityBuilder.setMode(HttpMultipartMode.BROWSER_COMPATIBLE)
      for ((key, value) <- strContents) {
        val strContent = ContentType.create("text/plain", Charset.forName("UTF-8"))
        mEntityBuilder.addTextBody(key, value, strContent)
      }
      for ((key, value) <- fileContents) {
        mEntityBuilder.addBinaryBody(key, value)
      }
      for ((key, value) <- inStreamContents) {
        //FIXME: use english
        // 使用此方式request-body中无数据 TODO:后续查找问题根源
        // val inputStreamBody = new InputStreamBody(value, ContentType.APPLICATION_OCTET_STREAM)
        // mEntityBuilder.addPart(key, inputStreamBody)
        mEntityBuilder.addBinaryBody(key, _inputStreamToByteArray(value), ContentType.DEFAULT_BINARY, "tmp.bin")
      }

      httpPost.setEntity(mEntityBuilder.build())

      val response = httpClient.execute(httpPost)
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode == HttpStatus.SC_OK) {
        val resEntity: HttpEntity = response.getEntity
        resStr = EntityUtils.toString(resEntity, "UTF-8")
      }
      response.close()
    } catch {
      case e: HttpHostConnectException =>
        throw new AipmServiceException(s"Failed connect to ${e.getHost} ")
      case e: Throwable => throw new AipmServiceException(e.getMessage)
    }
    resStr
  }

  private def _inputStreamToByteArray(is: InputStream): Array[Byte] = {
    val buf = ListBuffer[Byte]()
    var b = is.read()
    while (b != -1) {
      buf.append(b.byteValue)
      b = is.read()
    }
    buf.toArray
  }

}

class AipmServiceException(val msg: String)
  extends Exception(msg) {
}
