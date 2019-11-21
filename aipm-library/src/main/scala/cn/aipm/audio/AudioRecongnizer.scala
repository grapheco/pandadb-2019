package cn.aipm.audio

import cn.pandadb.cypherplus.PropertyExtractor
import cn.aipm.service.ServiceInitializer
import cn.pandadb.blob.Blob


class AudioRecongnizer extends PropertyExtractor with ServiceInitializer {

  override def declareProperties() = Map("content" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val content = service.mandarinASR(is)
    Map("content" -> content)
  })

}
