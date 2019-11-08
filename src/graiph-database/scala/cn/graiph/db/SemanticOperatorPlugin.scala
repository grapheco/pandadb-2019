package cn.graiph.db

import java.io.File

import cn.graiph.{ValueMatcher, CustomPropertyProvider, CypherPluginRegistry}
import cn.graiph.util.Logging
import org.neo4j.kernel.impl.{CustomDatabaseLifecyclePluginContext, CustomDatabaseLifecyclePlugin}
import org.springframework.context.support.FileSystemXmlApplicationContext

/**
  * Created by bluejoe on 2019/11/7.
  */
class SemanticOperatorPlugin extends CustomDatabaseLifecyclePlugin with Logging {
  override def init(ctx: CustomDatabaseLifecyclePluginContext): Unit = {
    val configuration = ctx.configuration;
    val cypherPluginRegistry = configuration.getRaw("blob.plugins.conf").map(x => {
      val xml = new File(x);

      val path =
        if (xml.isAbsolute) {
          xml.getPath
        }
        else {
          val configFilePath = configuration.getRaw("config.file.path")
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

    val customPropertyProvider = cypherPluginRegistry.createCustomPropertyProvider(configuration);
    val valueMatcher = cypherPluginRegistry.createValueComparatorRegistry(configuration);

    ctx.instanceContext.put[CustomPropertyProvider](customPropertyProvider);
    ctx.instanceContext.put[ValueMatcher](valueMatcher);
  }

  override def stop(ctx: CustomDatabaseLifecyclePluginContext): Unit = {

  }

  override def start(ctx: CustomDatabaseLifecyclePluginContext): Unit = {

  }
}