package cn.aipm.image

import cn.graiph.cypherplus.PropertyExtractor
import cn.aipm.service.ServiceInitializer
import cn.graiph.blob.Blob


class PlateNumberExtractor extends PropertyExtractor with ServiceInitializer {

  override def declareProperties() = Map("plateNumber" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val plateNumber = service.extractPlateNumber(is)
    Map("plateNumber" -> plateNumber)
  })

}