package cn.aipm.image

import javax.imageio.ImageIO

import cn.pandadb.cypherplus.PropertyExtractor
import cn.pandadb.blob.Blob
import cn.pandadb.util.{ContextMap, Configuration}

/**
  * Created by bluejoe on 2019/2/17.
  */
class ImageMetaDataExtractor extends PropertyExtractor {
  override def declareProperties() = Map("width" -> classOf[Int], "height" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream((is) => {
    val srcImage = ImageIO.read(is);
    Map("height" -> srcImage.getHeight(), "width" -> srcImage.getWidth());
  })

  override def initialize(conf: ContextMap): Unit = {

  }
}