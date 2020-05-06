package cn.pandadb.driver.values

case class Relationship(id: Long,
                        props: Map[String, AnyRef],
                        startNode: Node,
                        endNode: Node,
                        relationshipType: RelationshipType) extends Serializable {

  override def equals(o: Any): Boolean = {
    o.isInstanceOf[Relationship] && this.id == o.asInstanceOf[Relationship].id
  }
}

