package cn.pandadb.lifecycle

/**
 * Lifecycle interface for components.
 */

trait Lifecycle {
  @throws[Throwable]
  def init(): Unit

  @throws[Throwable]
  def start(): Unit

  @throws[Throwable]
  def stop(): Unit

  @throws[Throwable]
  def shutdown(): Unit
}