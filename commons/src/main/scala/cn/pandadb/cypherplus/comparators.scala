package cn.pandadb.cypherplus

import cn.pandadb.util.{Configuration}

trait AnyComparator {
  def initialize(conf: Configuration);
}

trait ValueComparator extends AnyComparator {
  def compare(a: Any, b: Any): Double;
}

trait SetComparator extends AnyComparator {
  def compareAsSets(a: Any, b: Any): Array[Array[Double]];
}

/**
  * Created by bluejoe on 2019/1/31.
  */
trait CustomPropertyProvider {
  def getCustomProperty(x: Any, propertyName: String): Option[Any];
}

trait ValueMatcher {
  def like(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean];

  def containsOne(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean];

  def containsSet(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean];

  /**
    * compares two values
    */
  def compareOne(a: Any, b: Any, algoName: Option[String]): Option[Double];

  /**
    * compares two objects as sets
    */
  def compareSet(a: Any, b: Any, algoName: Option[String]): Option[Array[Array[Double]]];
}
