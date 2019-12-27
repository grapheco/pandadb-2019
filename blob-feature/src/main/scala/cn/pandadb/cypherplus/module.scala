package cn.pandadb.cypherplus

import java.io.File

import cn.pandadb.blob.CypherPluginRegistry
import cn.pandadb.util._
import org.springframework.context.support.FileSystemXmlApplicationContext

class CypherPlusModule extends PandaModule with Logging {
  override def init(ctx: PandaModuleContext): Unit = {
    val conf = ctx.configuration;
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

    CypherPlusContext.bindCustomPropertyProvider(customPropertyProvider);
    CypherPlusContext.bindValueMatcher(valueMatcher);
  }

  override def stop(ctx: PandaModuleContext): Unit = {

  }

  override def start(ctx: PandaModuleContext): Unit = {

  }
}

object CypherPlusContext extends ContextMap {
  def customPropertyProvider: CustomPropertyProvider = get[CustomPropertyProvider]();

  def bindCustomPropertyProvider(customPropertyProvider: CustomPropertyProvider): Unit = put[CustomPropertyProvider](customPropertyProvider);

  def valueMatcher: ValueMatcher = get[ValueMatcher]();

  def bindValueMatcher(valueMatcher: ValueMatcher): Unit = put[ValueMatcher](valueMatcher);
}