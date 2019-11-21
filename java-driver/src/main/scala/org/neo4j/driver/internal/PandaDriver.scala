package org.neo4j.driver.internal

import org.neo4j.driver.Config
import org.neo4j.driver.internal.cluster.RoutingSettings
import org.neo4j.driver.internal.metrics.MetricsProvider
import org.neo4j.driver.internal.retry.RetryLogic
import org.neo4j.driver.internal.security.SecurityPlan
import org.neo4j.driver.internal.shaded.io.netty.util.concurrent.EventExecutorGroup
import org.neo4j.driver.internal.spi.ConnectionPool

/**
  * Created by bluejoe on 2019/11/21.
  */
object PandaDriver {
  def create(securityPlan: SecurityPlan, address: BoltServerAddress, connectionPool: ConnectionPool,
             eventExecutorGroup: EventExecutorGroup, routingSettings: RoutingSettings, retryLogic: RetryLogic, metricsProvider: MetricsProvider, config: Config): InternalDriver = {
    new PandaDriver(securityPlan: SecurityPlan, address: BoltServerAddress, connectionPool: ConnectionPool,
      eventExecutorGroup: EventExecutorGroup, routingSettings: RoutingSettings, retryLogic: RetryLogic, metricsProvider: MetricsProvider, config: Config)
  }
}

class PandaDriver(securityPlan: SecurityPlan, address: BoltServerAddress, connectionPool: ConnectionPool,
                  eventExecutorGroup: EventExecutorGroup, routingSettings: RoutingSettings, retryLogic: RetryLogic, metricsProvider: MetricsProvider, config: Config)
  extends InternalDriver(securityPlan: SecurityPlan, null, metricsProvider: MetricsProvider, config.logging) {

}
