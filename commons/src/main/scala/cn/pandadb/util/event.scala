package cn.pandadb.util

import scala.collection.mutable.ArrayBuffer

trait PandaEvent {
}

object PandaEventHub {
  type PandaEventHandler = PartialFunction[PandaEvent, Unit];
  val handlers = ArrayBuffer[PandaEventHandler]();

  def trigger(handler: PandaEventHandler): Unit = handlers += handler;

  def publish(event: PandaEvent): Unit = {
    handlers.filter(_.isDefinedAt(event)).foreach(_.apply(event))
  }
}