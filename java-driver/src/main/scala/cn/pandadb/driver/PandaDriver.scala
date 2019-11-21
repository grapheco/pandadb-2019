package cn.pandadb.driver

import java.util
import java.util.concurrent.{CompletableFuture, CompletionStage}

import cn.pandadb.network.ClusterManager
import org.neo4j.driver._
import org.neo4j.driver.async.AsyncSession
import org.neo4j.driver.internal.SessionConfig
import org.neo4j.driver.reactive.RxSession
import org.neo4j.driver.types.TypeSystem

import scala.xml.dtd.EMPTY

/**
  * Created by bluejoe on 2019/11/21.
  */
object PandaDriver {
  def create(uri: String, authToken: AuthToken, config: Config): Driver = {
    new PandaDriver(uri, authToken, config)
  }
}

class PandaDriver(uri: String, authToken: AuthToken, config: Config) extends Driver {
  val clusterOperator: ClusterManager = createClusterOperator(uri);

//  val defaultSessionConfig = new SessionConfig()
  val defaultSessionConfig = SessionConfig.empty()
  override def closeAsync(): CompletionStage[Void] = {
    //TODO
    new CompletableFuture[Void]();
  }

  override def session(): Session = session(defaultSessionConfig)

  override def session(sessionConfig: SessionConfig): Session = new PandaSession(sessionConfig, clusterOperator);

  override def defaultTypeSystem(): TypeSystem = ???

  override def rxSession(): RxSession = ???

  override def rxSession(sessionConfig: SessionConfig): RxSession = ???

  override def verifyConnectivityAsync(): CompletionStage[Void] = ???

  override def verifyConnectivity(): Unit = ???

  override def metrics(): Metrics = ???

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
  override def writeTransaction[T](work: TransactionWork[T]): T = ???

  override def writeTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???

  override def readTransaction[T](work: TransactionWork[T]): T = ???

  override def readTransaction[T](work: TransactionWork[T], config: TransactionConfig): T = ???

  override def run(statement: String, config: TransactionConfig): StatementResult = ???

  override def run(statement: String, parameters: util.Map[String, AnyRef], config: TransactionConfig): StatementResult = ???

  override def run(statement: Statement, config: TransactionConfig): StatementResult = ???

  override def close(): Unit = ???

  override def lastBookmark(): String = ???

  override def reset(): Unit = ???

  override def beginTransaction(): Transaction = ???

  override def beginTransaction(config: TransactionConfig): Transaction = ???

  override def run(statementTemplate: String, parameters: Value): StatementResult = ???

  override def run(statementTemplate: String, statementParameters: util.Map[String, AnyRef]): StatementResult = ???

  override def run(statementTemplate: String, statementParameters: Record): StatementResult = ???

  override def run(statementTemplate: String): StatementResult = ???

  override def run(statement: Statement): StatementResult = ???

  override def isOpen: Boolean = ???
}