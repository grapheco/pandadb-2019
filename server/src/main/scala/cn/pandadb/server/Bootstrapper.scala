package cn.pandadb.server

import java.io.File

trait Bootstrapper {

  def start(configFile: Option[File], configOverrides: Map[String, String]);

  def stop();

}
