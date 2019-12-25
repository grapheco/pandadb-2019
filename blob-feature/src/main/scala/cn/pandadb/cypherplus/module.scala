package cn.pandadb.cypherplus

import java.io.File

import cn.pandadb.blob.CypherPluginRegistry
import cn.pandadb.context.{InstanceBoundServiceFactoryRegistry, InstanceBoundService, InstanceBoundServiceContext, InstanceBoundServiceFactory}
import cn.pandadb.util._
import org.springframework.context.support.FileSystemXmlApplicationContext

class CypherPlusModule extends PandaModule {
  override def init(ctx: PandaModuleContext): Unit = {
    ctx.declareProperty(new CypherPluginPropertyParser())
  }

  override def stop(ctx: PandaModuleContext): Unit = {

  }

  override def start(ctx: PandaModuleContext): Unit = {

  }
}

class CypherPluginPropertyParser extends PropertyParser with Logging {
  override def parse(conf: Configuration): Iterable[Pair[String, _]] = {
    val cypherPluginRegistry = conf.getRaw("blob.plugins.conf").map(x => {
      val xml = new File(x);

      val path =
        if (xml.isAbsolute) {
          xml.getPath
        }
        else {
          val configFilePath = conf.getRaw("config.file.path")
          if (configFilePath.isDefined) {
            new File(new File(configFilePath.get).getParentFile, x).getAbsoluteFile.getCanonicalPath
          }
          else {
            xml.getAbsoluteFile.getCanonicalPath
          }
        }

      logger.info(s"loading semantic plugins: $path");
      val appctx = new FileSystemXmlApplicationContext("file:" + path);
      appctx.getBean[CypherPluginRegistry](classOf[CypherPluginRegistry]);
    }).getOrElse {
      logger.info(s"semantic plugins not loaded: blob.plugins.conf=null");
      new CypherPluginRegistry()
    }

    val customPropertyProvider = cypherPluginRegistry.createCustomPropertyProvider(conf);
    val valueMatcher = cypherPluginRegistry.createValueComparatorRegistry(conf);

    Array(
      classOf[CustomPropertyProvider].getName -> customPropertyProvider,
      classOf[ValueMatcher].getName -> valueMatcher
    )
  }
}