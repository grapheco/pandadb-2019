package cn.pandadb.driver

import java.io.IOException
import java.net.URI
import java.security.GeneralSecurityException
import java.{security, util}
import java.util.concurrent.{CompletableFuture, CompletionStage}

import cn.pandadb.network.ClusterManager
import org.neo4j.driver.Config.TrustStrategy
import org.neo4j.driver._
import org.neo4j.driver.async.{AsyncSession, AsyncStatementRunner, AsyncTransaction, AsyncTransactionWork, StatementResultCursor}
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.internal.async.connection.BootstrapFactory
import org.neo4j.driver.internal.cluster.{RoutingContext, RoutingSettings}
import org.neo4j.driver.internal.{BoltServerAddress, DirectConnectionProvider, DriverFactory, SessionConfig, SessionFactory, SessionFactoryImpl}
import org.neo4j.driver.internal.metrics.{InternalMetricsProvider, MetricsProvider}
import org.neo4j.driver.internal.retry.{ExponentialBackoffRetryLogic, RetryLogic, RetrySettings}
import org.neo4j.driver.internal.security.SecurityPlan
import org.neo4j.driver.internal.spi.ConnectionProvider
import org.neo4j.driver.internal.types.InternalTypeSystem
import org.neo4j.driver.internal.util.{Clock, Futures}
import org.neo4j.driver.reactive.{RxSession, RxStatementResult, RxTransaction, RxTransactionWork}
import org.neo4j.driver.types.TypeSystem
import org.reactivestreams.Publisher



/**
  * Created by bluejoe on 2019/11/21.
  */
object PandaDriver {
  def create(uri: String, authToken: AuthToken,config: Config): Driver = {
    new PandaDriver(uri, authToken,config)
  }
}

class PandaDriver(uri: String, authToken: AuthToken,config: Config) extends Driver {
  val clusterOperator: ClusterManager = createClusterOperator(uri);
//  val defaultSessionConfig = new SessionConfig()
  val defaultSessionConfig = SessionConfig.empty()
  override def closeAsync(): CompletionStage[Void] = {
    //TODO
    new CompletableFuture[Void]();
  }

  override def session(): Session = session(defaultSessionConfig)

  override def session(sessionConfig: SessionConfig): Session = new PandaSession(sessionConfig, clusterOperator);

  //override def defaultTypeSystem(): TypeSystem = ???
  override def defaultTypeSystem(): TypeSystem = InternalTypeSystem.TYPE_SYSTEM

  override def rxSession(): RxSession = ???


  override def rxSession(sessionConfig: SessionConfig): RxSession = ???

  /**
   *   verifyConnectivityAsync and verifyConnectivity  is not right ,  because uri is zkString
   */

  override def verifyConnectivityAsync(): CompletionStage[Void] = ???


  //override def verifyConnectivity(): Unit = ???
  override def verifyConnectivity(): Unit = {
    Futures.blockingGet(this.verifyConnectivityAsync())
  }


  //override def metrics(): Metrics = ???
  override def metrics(): Metrics = {
    createDriverMetrics(config, this.createClock()).metrics()
  }
  def createDriverMetrics(config:Config , clock : Clock ):MetricsProvider= {
    if (config.isMetricsEnabled()) new InternalMetricsProvider(clock) else MetricsProvider.METRICS_DISABLED_PROVIDER
  }
  def createClock():Clock = {
    Clock.SYSTEM
  }




  override def asyncSession(): AsyncSession = ???

  override def asyncSession(sessionConfig: SessionConfig): AsyncSession = ???

  override def close(): Unit = {

  }

  override def isEncrypted: Boolean = ???




  private def createClusterOperator(uri: String): ClusterManager = {
    null
  }
}

class PandaSession(sessionConfig: SessionConfig, clusterOperator: ClusterManager) extends Session {

  val host = "10.0.86.179"

  val port = 7687
  val uri = s"bolt://$host:$port"
  val driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "neo4j"))
  val session = driver.session(sessionConfig)

  //override def writeTransaction[T](work: TransactionWork[T]): T = ???
  override def writeTransaction[T](work: TransactionWork[T]): T = {
    session.writeTransaction(work)
  }

  //override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???
  override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = {
    session.writeTransaction(work,config)
  }

  //override def readTransaction[T](work: TransactionWork[T]): T = ???
  override def readTransaction[T](work: TransactionWork[T]): T = {
    session.readTransaction(work)
  }

  //override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???
  override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = {
    session.readTransaction(work,config)
  }

  //override def run(statement: String, config: TransactionConfig): StatementResult = ???
  override def run(statement: String, config: TransactionConfig): StatementResult = {
    session.run(statement,config)
  }

  //override def run(statement: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): StatementResult = ???
  override def run(statement: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): StatementResult = {
    session.run(statement,parameters,config)
  }
  //override def run(statement: Statement, config: TransactionConfig): StatementResult = ???
  override def run(statement: Statement, config: TransactionConfig): StatementResult = {
    session.run(statement,config)
  }

  //override def close(): Unit = ???
  override def close(): Unit = {
    session.close()
  }

  //override def lastBookmark(): String = ???
  override def lastBookmark(): String = {
    session.lastBookmark()
  }

  //override def reset(): Unit = ???
  override def reset(): Unit = {
    session.reset()
  }

  //override def beginTransaction(): Transaction = ???
  override def beginTransaction(): Transaction = {
    session.beginTransaction()
  }

 // override def beginTransaction(config: TransactionConfig): Transaction = ???
 override def beginTransaction(config: TransactionConfig): Transaction = {
   session.beginTransaction(config)
 }

 // override def run(statementTemplate: String, parameters: Value): StatementResult = ???
 override def run(statementTemplate: String, parameters: Value): StatementResult = {
   session.run(statementTemplate,parameters)
 }


  //override def run(statementTemplate: String, statementParameters: util.Map[String, AnyRef]): StatementResult = ???
  override def run(statementTemplate: String, statementParameters: util.Map[String, AnyRef]): StatementResult = {
    session.run(statementTemplate,statementParameters)
  }

  //override def run(statementTemplate: String, statementParameters: Record): StatementResult = ???
  override def run(statementTemplate: String, statementParameters: Record): StatementResult = {
    session.run(statementTemplate,statementParameters)
  }

  //override def run(statementTemplate: String): StatementResult = ???
  override def run(statementTemplate: String): StatementResult = {
    session.run(statementTemplate)
  }

  //override def run(statement: Statement): StatementResult = ???
  override def run(statement: Statement): StatementResult = {
    session.run(statement)
  }

  //override def isOpen: Boolean = ???
  override def isOpen: Boolean = {
    session.isOpen
  }
}