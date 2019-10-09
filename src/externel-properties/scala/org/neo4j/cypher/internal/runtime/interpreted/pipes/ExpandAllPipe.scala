package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.v3_5.util.InternalException
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{RelationshipValue, NodeValue}

case class ExpandAllPipe(source: Pipe,
                         fromName: String,
                         relName: String,
                         toName: String,
                         dir: SemanticDirection,
                         types: LazyTypes)
                        (val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap {
      row =>
        getFromNode(row) match {
          case n: NodeValue =>
            val relationships: Iterator[RelationshipValue] = state.query.getRelationshipsForIds(n.id(), dir, types.types(state.query))
            relationships.map { r =>
              val other = r.otherNode(n)
              executionContextFactory.copyWith(row, relName, r, toName, other)
            }

          case Values.NO_VALUE => None

          case value => throw new InternalException(s"Expected to find a node at '$fromName' but found $value instead")
        }
    }
  }

  def typeNames = types.names

  def getFromNode(row: ExecutionContext): AnyValue =
    row.getOrElse(fromName, throw new InternalException(s"Expected to find a node at '$fromName' but found nothing"))
}
