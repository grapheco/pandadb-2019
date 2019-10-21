package cn.aipm.text

import cn.graiph.ValueComparator
import info.debatty.java.stringsimilarity.{Cosine, Jaccard, JaroWinkler}
import org.neo4j.blob.utils.Configuration

/**
  * Created by bluejoe on 2019/2/17.
  */
class JaroWinklerStringSimilarity extends ValueComparator {
  def compare(str1: Any, str2: Any): Double = {
    val jw = new JaroWinkler();
    jw.similarity(str1.asInstanceOf[String], str2.asInstanceOf[String]);
  }

  override def initialize(conf: Configuration): Unit = {

  }
}

class JaccardStringSimilarity extends ValueComparator {
  def compare(str1: Any, str2: Any): Double = {
    val jw = new Jaccard();
    jw.similarity(str1.asInstanceOf[String], str2.asInstanceOf[String]);
  }

  override def initialize(conf: Configuration): Unit = {

  }
}

class CosineStringSimilarity extends ValueComparator {
  def compare(str1: Any, str2: Any): Double = {
    val jw = new Cosine();
    jw.similarity(str1.asInstanceOf[String], str2.asInstanceOf[String]);
  }

  override def initialize(conf: Configuration): Unit = {

  }
}