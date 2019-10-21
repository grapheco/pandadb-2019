package cn.graiph.driver

import org.neo4j.blob.utils.Logging
import org.neo4j.driver.{Record, Session, StatementResult}

import scala.reflect.ClassTag

/**
  * Created by bluejoe on 2019/7/26.
  */
trait CypherService extends Logging {
  def queryObjects[T: ClassTag](queryString: String, fnMap: (Record => T)): Iterator[T];

  def execute[T](f: (Session) => T): T;

  def executeQuery[T](queryString: String, fn: (StatementResult => T)): T;

  def executeQuery[T](queryString: String, params: Map[String, AnyRef], fn: (StatementResult => T)): T;

  def executeUpdate(queryString: String);

  def executeUpdate(queryString: String, params: Map[String, AnyRef]);

  final def executeQuery[T](queryString: String, params: Map[String, AnyRef]): Unit =
    executeQuery(queryString, params, (StatementResult) => {
      null.asInstanceOf[T]
    })

  final def querySingleObject[T](queryString: String, fnMap: (Record => T)): T = {
    executeQuery(queryString, (rs: StatementResult) => {
      fnMap(rs.next());
    });
  }

  final def querySingleObject[T](queryString: String, params: Map[String, AnyRef], fnMap: (Record => T)): T = {
    executeQuery(queryString, params, (rs: StatementResult) => {
      fnMap(rs.next());
    });
  }
}
