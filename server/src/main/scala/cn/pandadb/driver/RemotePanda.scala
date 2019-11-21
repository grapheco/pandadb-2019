package cn.pandadb.driver

import cn.pandadb.util.Logging

/**
  * Created by bluejoe on 2019/7/17.
  */
object RemotePanda extends Logging {
  def connect(url: String, user: String = "", pass: String = ""): CypherService = {
    new BoltService(url, user, pass);
  }
}
