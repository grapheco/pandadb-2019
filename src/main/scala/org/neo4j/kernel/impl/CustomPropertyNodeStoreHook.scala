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

      //TODO: update modified nodes
      val mn = state.modifiedNodes();
      //_propertyNodeStore.updateNodes
    }

    new TransactionHook.Outcome {
      override def failure(): Throwable = null

      override def isSuccessful: Boolean = true
    }
  }
}
