package cn.pandadb.cypherplus

import cn.pandadb.util.{ContextMap}

/**
  * Created by bluejoe on 2019/7/22.
  */
trait PropertyExtractor {
  def declareProperties(): Map[String, Class[_]];

  def initialize(conf: ContextMap);

  def extract(value: Any): Map[String, Any];
}
