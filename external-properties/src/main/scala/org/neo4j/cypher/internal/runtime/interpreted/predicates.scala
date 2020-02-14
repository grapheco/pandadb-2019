package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.values.AnyValue

/**
  * Created by bluejoe on 2019/10/7.
  */
trait NFExpression {
}

trait NFPredicate extends NFExpression {
}

case class NFGreaterThan(propName: String, value: AnyValue) extends NFPredicate {
}

case class NFGreaterThanOrEqual(propName: String, value: AnyValue) extends NFPredicate {
}

case class NFLessThan(propName: String, value: AnyValue) extends NFPredicate {
}

case class NFLessThanOrEqual(propName: String, value: AnyValue) extends NFPredicate {
}

case class NFEquals(propName: String, value: AnyValue) extends NFPredicate {
}

case class NFNotEquals(propName: String, value: AnyValue) extends NFPredicate {
}

case class NFNotNull(propName: String) extends NFPredicate {
}

case class NFIsNull(propName: String) extends NFPredicate {
}

case class NFTrue() extends NFPredicate {
}

case class NFFalse() extends NFPredicate {
}

case class NFStartsWith(propName: String, text: String) extends NFPredicate {
}

case class NFEndsWith(propName: String, text: String) extends NFPredicate {
}

case class NFHasProperty(propName: String) extends NFPredicate {
}

case class NFContainsWith(propName: String, text: String) extends NFPredicate {
}

case class NFRegexp(propName: String, text: String) extends NFPredicate {
}

case class NFAnd(a: NFPredicate, b: NFPredicate) extends NFPredicate {
}

case class NFOr(a: NFPredicate, b: NFPredicate) extends NFPredicate {
}

case class NFNot(a: NFPredicate) extends NFPredicate {
}

case class NFConstantCachedIn(a: NFPredicate) extends NFPredicate {
}