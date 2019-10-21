package cn.aipm.text

import cn.aipm.service.ServiceInitializer
import cn.graiph.PropertyExtractor

class SentimentClassifier extends PropertyExtractor with ServiceInitializer {

  override def declareProperties() = Map("sentiment" -> classOf[String])

  override def extract(text: Any): Map[String, Any] = {

    val sentiment = service.sentimentClassifier(text.asInstanceOf[String])
    Map("sentiment" -> sentiment)
  }

}

