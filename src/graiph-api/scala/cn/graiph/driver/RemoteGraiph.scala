package cn.graiph.driver

import org.neo4j.blob.utils.Logging

/**
  * Created by bluejoe on 2019/7/17.
  */
object RemoteGraiph extends Logging {
  def connect(url: String, user: String = "", pass: String = ""): CypherService = {
    new BoltService(url, user, pass);
  }
}
