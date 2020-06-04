package cn.pandadb.server

import java.nio.ByteBuffer

import cn.pandadb.cluster.ClusterService
import org.grapheco.hippo.{ChunkedStream, CompleteStream, HippoRpcHandler, ReceiveContext}
import cn.pandadb.configuration.{Config => PandaConfig}


import scala.collection.mutable.ArrayBuffer

class PandaRpcHandler(pandaConfig: PandaConfig, clusterService: ClusterService) extends HippoRpcHandler {
  val lst = ArrayBuffer[HippoRpcHandler]()

  def add(handler: HippoRpcHandler): Unit = {
    lst += handler
  }

  override def receiveWithBuffer(extraInput: ByteBuffer, context: ReceiveContext): PartialFunction[Any, Unit] = {
    case message => {
      val res = lst.filter(_.receiveWithBuffer(extraInput, context).isDefinedAt(message))
      res(0).receiveWithBuffer(extraInput, context).apply(message)
    }
  }

  override def openCompleteStream(): PartialFunction[Any, CompleteStream] = {
    case message => {
      val res = lst.filter(_.openCompleteStream().isDefinedAt(message))
      res(0).openCompleteStream().apply(message)
    }
  }

  override def openChunkedStream(): PartialFunction[Any, ChunkedStream] = {
    case message => {
      val res = lst.filter(_.openChunkedStream().isDefinedAt(message))
      res(0).openChunkedStream().apply(message)
    }
  }
}
