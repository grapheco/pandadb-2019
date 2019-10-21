package cn.aipm.image

import cn.aipm.service.ServiceInitializer
import cn.graiph.PropertyExtractor
import org.neo4j.blob.Blob

class DogOrCatClassifier extends PropertyExtractor with ServiceInitializer {

  override def declareProperties() = Map("animal" -> classOf[String])

  override def extract(x: Any): Map[String, Any] = x.asInstanceOf[Blob].offerStream(is => {
    val animal = service.classifyAnimal(is)
    Map("animal" -> animal)
  })

}
