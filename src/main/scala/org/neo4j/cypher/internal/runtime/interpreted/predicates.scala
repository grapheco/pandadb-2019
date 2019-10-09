package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.values.AnyValue

/**
  * Created by bluejoe on 2019/10/7.
  */
trait NodeFieldPredicate {
}

case class NodeFieldGreaterThan(fieldName: String, value: AnyValue) extends NodeFieldPredicate {
}

case class NodeFieldLessThan(fieldName: String, value: AnyValue) extends NodeFieldPredicate {
}
