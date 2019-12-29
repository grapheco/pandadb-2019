package cn.pandadb.context

import java.io.File

import cn.pandadb.util.PandaEvent
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.factory.DatabaseInfo
import org.neo4j.kernel.impl.proc.Procedures

case class GraphDatabaseStartedEvent(proceduresService: Procedures,
                                     storeDir: File,
                                     neo4jConf: Config,
                                     databaseInfo: DatabaseInfo)
  extends PandaEvent {

}
