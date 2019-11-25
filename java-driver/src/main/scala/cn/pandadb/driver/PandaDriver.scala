package cn.pandadb.driver

import java.io.IOException
import java.net.URI
import java.security.GeneralSecurityException
import java.{security, util}
import java.util.concurrent.{CompletableFuture, CompletionStage}

import cn.pandadb.network.{ClusterClient, NodeAddress}
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
  def create(uri: String, authToken: AuthToken, config: Config): Driver = {
    new PandaDriver(uri, authToken, config)
  }
}

class PandaDriver(uri: String, authToken: AuthToken, config: Config) extends Driver {
  val clusterClient: ClusterClient = createClusterClient(uri);
//  val defaultSessionConfig = new SessionConfig()
  val defaultSessionConfig = SessionConfig.empty()
  override def closeAsync(): CompletionStage[Void] = {
    //TODO
    new CompletableFuture[Void]();
  }

  override def session(): Session = session(defaultSessionConfig)

  override def session(sessionConfig: SessionConfig): Session = new PandaSession(sessionConfig, clusterClient);

  //override def defaultTypeSystem(): TypeSystem = ???
  override def defaultTypeSystem(): TypeSystem = InternalTypeSystem.TYPE_SYSTEM

  //override def rxSession(): RxSession = ???
  override def rxSession(): RxSession = null



  //override def rxSession(sessionConfig: SessionConfig): RxSession = ???
  override def rxSession(sessionConfig: SessionConfig): RxSession = null


  /**
   *   verifyConnectivityAsync and verifyConnectivity  is not right ,  because uri is zkString
   */

  //override def verifyConnectivityAsync(): CompletionStage[Void] = ???
  override def verifyConnectivityAsync(): CompletionStage[Void] = null



  //override def verifyConnectivity(): Unit = ???
  override def verifyConnectivity(): Unit = {
    Futures.blockingGet(this.verifyConnectivityAsync())
  }


  //override def metrics(): Metrics = ???
  override def metrics(): Metrics = {
    createDriverMetrics(config, this.createClock()).metrics()
  }
  private def createDriverMetrics(config: Config, clock: Clock ): MetricsProvider = {
    if (config.isMetricsEnabled()) new InternalMetricsProvider(clock) else MetricsProvider.METRICS_DISABLED_PROVIDER
  }
  private def createClock(): Clock = {
    Clock.SYSTEM
  }



  //override def asyncSession(): AsyncSession = ???
  override def asyncSession(): AsyncSession = null

  //override def asyncSession(sessionConfig: SessionConfig): AsyncSession = ???
  override def asyncSession(sessionConfig: SessionConfig): AsyncSession = null


  override def close(): Unit = {

  }


  //wait to finish
  //override def isEncrypted: Boolean = ???
  override def isEncrypted: Boolean = true




  private def createClusterClient(uri: String): ClusterClient = {
    null
  }
}

class PandaSession(sessionConfig: SessionConfig, clusterOperator: ClusterClient) extends Session {

  var host = "10.0.86.179"

  var port = 7687
  var uri = s"bolt://$host:$port"
  var driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "123456"))
  //var driver = GraphDatabase.driver(uri)
  var session = driver.session(sessionConfig)

  private def isRead(statement: String): Boolean = {
    val tempStatement = statement.toLowerCase()
    if (tempStatement.contains("create") || tempStatement.contains("merge") ||
      tempStatement.contains("set") || tempStatement.contains("delete")) {
      false
    }
    else true
  }
  private def getWriteNode(): NodeAddress = {
    clusterOperator.getWriteMasterNode()
  }
  private def getReadNode(): NodeAddress = {
    //random to pick up a node
    val nodeLists = clusterOperator.getAllNodes().toList
    val index = (new util.Random).nextInt(nodeLists.length)
    nodeLists(index)
  }
  private def getNode(statement: String): NodeAddress = {
    if (isRead(statement)) getReadNode() else getWriteNode()
  }
  private def getSessionReady(statement: String): Unit = {
    val node = getNode(statement)
    this.host = node.host
    this.port = node.port
    this.uri = s"bolt://$host:$port"
    this.driver = GraphDatabase.driver(uri, AuthTokens.basic("neo4j", "neo4j"))
    this.session = driver.session(sessionConfig)

  }
  //override def writeTransaction[T](work: TransactionWork[T]): T = ???
  override def writeTransaction[T](work: TransactionWork[T]): T = {
    session.writeTransaction(work)
  }

  //override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???
  override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = {
    session.writeTransaction(work, config)
  }

  //override def readTransaction[T](work: TransactionWork[T]): T = ???
  override def readTransaction[T](work: TransactionWork[T]): T = {
    session.readTransaction(work)
  }

  //override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???
  override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = {
    session.readTransaction(work, config)
  }

  //override def run(statement: String, config: TransactionConfig): StatementResult = ???
  override def run(statement: String, config: TransactionConfig): StatementResult = {
    session.run(statement, config)
  }

  //override def run(statement: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): StatementResult = ???
  override def run(statement: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): StatementResult = {
    session.run(statement, parameters, config)
  }
  //override def run(statement: Statement, config: TransactionConfig): StatementResult = ???
  override def run(statement: Statement, config: TransactionConfig): StatementResult = {
    session.run(statement, config)
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
   session.run(statementTemplate, parameters)
 }


  //override def run(statementTemplate: String, statementParameters: util.Map[String, AnyRef]): StatementResult = ???
  override def run(statementTemplate: String, statementParameters: util.Map[String, AnyRef]): StatementResult = {
    session.run(statementTemplate, statementParameters)
  }

  //override def run(statementTemplate: String, statementParameters: Record): StatementResult = ???
  override def run(statementTemplate: String, statementParameters: Record): StatementResult = {
    session.run(statementTemplate, statementParameters)
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