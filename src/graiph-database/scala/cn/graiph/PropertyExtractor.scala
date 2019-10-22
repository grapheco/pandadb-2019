package cn.graiph

import cn.graiph.util.Configuration

/**
  * Created by bluejoe on 2019/7/22.
  */
trait PropertyExtractor {
  def declareProperties(): Map[String, Class[_]];

  def initialize(conf: Configuration);

  def extract(value: Any): Map[String, Any];
}
