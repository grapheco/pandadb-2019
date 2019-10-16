package org.neo4j.kernel.impl

import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction
import org.neo4j.kernel.api.{KernelTransaction, TransactionHook}
import org.neo4j.storageengine.api.StorageReader
import org.neo4j.storageengine.api.txstate.ReadableTransactionState

import scala.collection.JavaConversions._

/**
  * Created by bluejoe on 2019/10/6.
  */
class CustomPropertyNodeStoreHook extends TransactionHook[TransactionHook.Outcome] {
  override def afterRollback(state: ReadableTransactionState, transaction: KernelTransaction, outcome: TransactionHook.Outcome): Unit = {
    //discard update
  }

  override def afterCommit(state: ReadableTransactionState, transaction: KernelTransaction, outcome: TransactionHook.Outcome): Unit = {
  }

  override def beforeCommit(state: ReadableTransactionState, transaction: KernelTransaction, storageReader: StorageReader): TransactionHook.Outcome = {
    if (state.hasDataChanges) {

      //save solr documents
      val aar = state.addedAndRemovedNodes();
      val tokens = transaction.tokenRead();

      //save added nodes
      val docsToBeAdded = aar.getAdded.collect(new LongToObjectFunction[CustomPropertyNode]() {
        override def valueOf(l: Long): CustomPropertyNode = {
          new CustomPropertyNode(l, state.getNodeState(l).addedProperties().map { prop =>
            val key = tokens.propertyKeyName(prop.propertyKeyId())
            val value = prop.value()
            key -> value
          }.toMap, state.getNodeState(l).labelDiffSets().getAdded.collect(new LongToObjectFunction[String] {
            override def valueOf(l: Long): String = tokens.nodeLabelName(l.toInt)
          }))
        }
      }).toList

      if (!docsToBeAdded.isEmpty) {
        CustomPropertyNodeStoreHolder.get.addNodes(docsToBeAdded);
      }

      //delete removed nodes
      //maybe wrong
      val docsToBeDeleted = aar.getRemoved.collect(new LongToObjectFunction[Long]() {
        override def valueOf(l: Long): Long = l
      }).toList

      if (!docsToBeDeleted.isEmpty) {
        CustomPropertyNodeStoreHolder.get.deleteNodes(docsToBeDeleted)
      }

      //TODO-1: update modified nodes
      val mn = state.modifiedNodes();
      //_propertyNodeStore.updateNodes
      if (mn.nonEmpty) {
        val docsToBeUpdated = mn.map(ns => {

          val id = ns.getId

          val fieldsAdded: Map[String, Value] = ns.addedProperties().map(prop => {
            val key = tokens.propertyKeyName(prop.propertyKeyId())
            val value = prop.value()
            key -> value
          }).toMap

          val fieldsRemoved: Iterable[String] = ns.removedProperties().collect(new IntToObjectFunction[String] {
            override def valueOf(intParameter: Int): String = tokens.nodeLabelName(intParameter)
          })

          val fieldsUpdated: Map[String, Value] = ns.changedProperties().map(prop => {
            val key = tokens.propertyKeyName(prop.propertyKeyId())
            val value = prop.value()
            key -> value
          }).toMap

          val labelsAdded: Iterable[String] = ns.labelDiffSets().getAdded.collect(new LongToObjectFunction[String] {
            override def valueOf(longParameter: Long): String = tokens.nodeLabelName(longParameter.toInt)
          })

          val labelsRemoved: Iterable[String] = ns.labelDiffSets().getRemoved.collect(new LongToObjectFunction[String] {
            override def valueOf(longParameter: Long): String = tokens.nodeLabelName(longParameter.toInt)
          })
          CustomPropertyNodeModification(id, fieldsAdded, fieldsRemoved, fieldsUpdated, labelsAdded, labelsRemoved)
        })

        CustomPropertyNodeStoreHolder.get.updateNodes(docsToBeUpdated)
      }
    }

    new TransactionHook.Outcome {
      override def failure(): Throwable = null

      override def isSuccessful: Boolean = true
    }
  }
}
