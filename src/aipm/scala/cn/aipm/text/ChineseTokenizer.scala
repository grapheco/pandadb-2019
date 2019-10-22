package cn.aipm.text

import cn.aipm.service.ServiceInitializer
import cn.graiph.PropertyExtractor

class ChineseTokenizer extends PropertyExtractor with ServiceInitializer {

  override def declareProperties() = Map("words" -> classOf[Array[String]])

  override def extract(text: Any): Map[String, Array[String]] = {
    val words = service.segmentText(text.asInstanceOf[String]).toArray
    Map("words" -> words)
  }

}