package cn.graiph

import org.neo4j.blob.utils.{Configuration, Logging}
import org.neo4j.blob.{Blob}

import scala.beans.BeanProperty
import scala.collection.mutable

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

object ValueType {
  def typeNameOf(x: Any): String = x match {
    case b: Blob => s"blob/${b.mimeType.major}".toLowerCase()
    case _ => x.getClass.getSimpleName.toLowerCase()
  }

  val ANY_BLOB = "blob/*";
  val ANY = "*";

  def typeNameOf(a: Any, b: Any): String = s"${typeNameOf(a)}:${typeNameOf(b)}"

  def concat(a: String, b: String): String = s"$a:$b"
}

class DomainExtractorEntry {
  @BeanProperty var domain: String = "";
  @BeanProperty var extractor: PropertyExtractor = _;
}

class DomainComparatorEntry {
  @BeanProperty var domain: String = "";
  //default threshold
  @BeanProperty var threshold: Double = 0.7;
  @BeanProperty var name: String = "";
  @BeanProperty var comparator: AnyComparator = _;
}

class CypherPluginRegistry {
  @BeanProperty var extractors: Array[DomainExtractorEntry] = Array();
  @BeanProperty var comparators: Array[DomainComparatorEntry] = Array();

  def createCustomPropertyProvider(conf: Configuration) = new CustomPropertyProvider {
    extractors.foreach(_.extractor.initialize(conf));

    //propertyName, typeName
    val map: Map[(String, String), Array[PropertyExtractor]] = extractors
      .flatMap(x => x.extractor.declareProperties().map(prop => (prop._1, x.domain) -> x.extractor))
      .groupBy(_._1)
      .map(x => x._1 -> x._2.map(_._2))

    //TODO: cache extraction
    def getCustomProperty(x: Any, name: String): Option[Any] = {
      //image:png
      val m1 = map.get(name -> ValueType.typeNameOf(x)).map(_.head.extract(x).apply(name));

      if (m1.isDefined) {
        m1
      }
      else {
        //blob:*
        val m3 =
          if (x.isInstanceOf[Blob]) {
            map.get(name -> ValueType.ANY_BLOB).map(_.head.extract(x).apply(name))
          }
          else {
            None
          }

        if (m3.isDefined) {
          m3
        }
        else {
          //*
          map.get(name -> ValueType.ANY)
            .map(_.head.extract(x).apply(name))
        }
      }
    }
  }

  def createValueComparatorRegistry(conf: Configuration) = new ValueMatcher with Logging {
    type CompareAnyMethod = (Any, Any) => Any;
    type CompareValueMethod = (Any, Any) => Double;
    type CompareSetMethod = (Any, Any) => Array[Array[Double]];

    //initialize all comparators
    comparators.map(_.comparator).foreach(_.initialize(conf));

    def like(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean] = {
      (a, b) match {
        case (null, null) => Some(true)
        case (Blob.EMPTY, Blob.EMPTY) => Some(true)
        case (null, _) => Some(false)
        case (_, null) => Some(false)
        case _ =>
          val (m: CompareValueMethod, e: DomainComparatorEntry) = getNotNullValueComparator(a, b, algoName)
          Some(m.apply(a, b) > threshold.getOrElse(e.threshold))
      }
    }

    override def compareOne(a: Any, b: Any, algoName: Option[String]): Option[Double] = {
      (a, b) match {
        case (null, null) => Some(1.0)
        case (null, _) => Some(0.0)
        case (_, null) => Some(0.0)
        case _ =>
          val (m: CompareValueMethod, e: DomainComparatorEntry) = getNotNullValueComparator(a, b, algoName)
          Some(m.apply(a, b))
      }
    }

    override def compareSet(a: Any, b: Any, algoName: Option[String]): Option[Array[Array[Double]]] = {
      (a, b) match {
        case (null, null) => Some(Array(Array(1.0)))
        case (null, _) => Some(Array(Array(0.0)))
        case (_, null) => Some(Array(Array(0.1)))
        case _ =>
          val (m: CompareSetMethod, e: DomainComparatorEntry) = getNotNullSetComparator(a, b, algoName)
          Some(m.apply(a, b))
      }
    }

    override def containsOne(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean] = {
      (a, b) match {
        case (null, null) => Some(true)
        case (null, _) => Some(false)
        case (_, null) => Some(true)
        case _ =>
          val (m: CompareValueMethod, e: DomainComparatorEntry) = getNotNullValueComparator(a, b, algoName)
          val r = m.apply(a, b)
          val th = threshold.getOrElse(e.threshold)
          Some(r > th)
      }
    }

    override def containsSet(a: Any, b: Any, algoName: Option[String], threshold: Option[Double]): Option[Boolean] = {
      (a, b) match {
        case (null, null) => Some(true)
        case (null, _) => Some(false)
        case (_, null) => Some(true)
        case _ =>
          val (m: CompareSetMethod, e: DomainComparatorEntry) = getNotNullSetComparator(a, b, algoName)
          val r = m.apply(a, b)
          val th = threshold.getOrElse(e.threshold)
          Some(!r.exists(_.max <= th))
      }
    }

    private def getMatchedComparator(compareValueOrSet: Boolean, typeA: String, typeB: String, algoName: Option[String]): Option[(CompareAnyMethod, DomainComparatorEntry)] = {
      def isEntryMatched(entry: DomainComparatorEntry, typeName: String): Boolean = {
        val f = entry.domain.equalsIgnoreCase(typeName) &&
          (if (compareValueOrSet)
            entry.comparator.isInstanceOf[ValueComparator]
          else
            entry.comparator.isInstanceOf[SetComparator]
            )

        algoName.map { name =>
          f && entry.name.equalsIgnoreCase(name)
        }.getOrElse {
          f
        }
      }

      def doCompare(comparator: AnyComparator, a: Any, b: Any): Any = {
        if (compareValueOrSet)
          comparator.asInstanceOf[ValueComparator].compare(a, b);
        else
          comparator.asInstanceOf[SetComparator].compareAsSets(a, b);
      }

      comparators.find(isEntryMatched(_, ValueType.concat(typeA, typeB)))
        .map(entry => ((x: Any, y: Any) => doCompare(entry.comparator, x, y)) -> entry)
        .orElse(comparators.find(isEntryMatched(_, ValueType.concat(typeB, typeA)))
          .map(entry => ((x: Any, y: Any) => doCompare(entry.comparator, y, x)) -> entry))
    }

    val _cachedValueComparators = mutable.Map[(String, String, Option[String]), Option[(CompareValueMethod, DomainComparatorEntry)]]();
    val _cachedSetComparators = mutable.Map[(String, String, Option[String]), Option[(CompareSetMethod, DomainComparatorEntry)]]();

    private def getNotNullValueComparator(a: Any, b: Any, algoName: Option[String]): (CompareValueMethod, DomainComparatorEntry) = {
      _cachedValueComparators.getOrElseUpdate((ValueType.typeNameOf(a), ValueType.typeNameOf(b), algoName), {
        val opt = getMatchedComparator(compareValueOrSet = true, ValueType.typeNameOf(a), ValueType.typeNameOf(b), algoName);
        opt.map((x) => x._1.asInstanceOf[CompareValueMethod] -> x._2)
          .orElse(
            getMatchedComparator(compareValueOrSet = false, ValueType.typeNameOf(a), ValueType.typeNameOf(b), algoName)
              .map(en => asCompareValueMethod(en._1.asInstanceOf[CompareSetMethod]) -> en._2))
      }
      ).getOrElse(throw new NoSuitableComparatorException(a, b, algoName))
    }

    private def getNotNullSetComparator(a: Any, b: Any, algoName: Option[String]): (CompareSetMethod, DomainComparatorEntry) = {
      _cachedSetComparators.getOrElseUpdate((ValueType.typeNameOf(a), ValueType.typeNameOf(b), algoName), {
        val opt = getMatchedComparator(compareValueOrSet = false, ValueType.typeNameOf(a), ValueType.typeNameOf(b), algoName);
        opt.map((x) => x._1.asInstanceOf[CompareSetMethod] -> x._2)
          .orElse(
            getMatchedComparator(compareValueOrSet = true, ValueType.typeNameOf(a), ValueType.typeNameOf(b), algoName)
              .map(en => asCompareSetMethod(en._1.asInstanceOf[CompareValueMethod]) -> en._2))
      }
      ).getOrElse(throw new NoSuitableComparatorException(a, b, algoName))
    }

    private def asCompareValueMethod(m: CompareSetMethod): CompareValueMethod = {
      (a: Any, b: Any) =>
        val r: Array[Array[Double]] = m(a, b);
        r.flatMap(x => x).max;
    }

    private def asCompareSetMethod(m: CompareValueMethod): CompareSetMethod = {
      (a: Any, b: Any) =>
        val r: Double = m(a, b);
        Array(Array(r))
    }
  }
}

class UnknownPropertyException(name: String, x: Any)
  extends RuntimeException(s"unknown property `$name` for $x") {

}

class NoSuitableComparatorException(a: Any, b: Any, algoName: Option[String])
  extends RuntimeException(s"no suiltable comparator: ${ValueType.typeNameOf(a)} and ${ValueType.typeNameOf(b)}, algorithm name: ${algoName.getOrElse("(none)")}") {

}

class TooManyObjectsException(o: Any)
  extends RuntimeException(s"too many objects: $o") {

}