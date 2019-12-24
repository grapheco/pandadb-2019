package cn.pandadb.cypherplus

import java.io.File

import cn.pandadb.blob.CypherPluginRegistry
import cn.pandadb.context.{InstanceBoundService, InstanceBoundServiceContext, InstanceBoundServiceFactory}
import cn.pandadb.util.Logging
import org.springframework.context.support.FileSystemXmlApplicationContext

/**
  * Created by bluejoe on 2019/11/7.
  */
class SemanticOperatorServiceFactory extends InstanceBoundServiceFactory with Logging {
  def create(ctx: InstanceBoundServiceContext): Option[InstanceBoundService] = {
    val configuration = ctx.instanceContext;
    val cypherPluginRegistry = configuration.getOption[String]("blob.plugins.conf").map(x => {
      val xml = new File(x);

      val path =
        if (xml.isAbsolute) {
          xml.getPath
        }
        else {
          val configFilePath = configuration.getOption[String]("config.file.path")
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

    None;
  }
}