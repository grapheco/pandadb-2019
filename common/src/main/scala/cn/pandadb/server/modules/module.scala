package cn.pandadb.server.modules

import cn.pandadb.configuration.Config
import cn.pandadb.lifecycle.LifecycleAdapter

trait LifecycleServerModule extends LifecycleAdapter {
}

trait LifecycleServer extends LifecycleAdapter {
  def getConfig(): Config
}

