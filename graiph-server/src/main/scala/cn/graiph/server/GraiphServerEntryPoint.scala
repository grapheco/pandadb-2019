package cn.graiph.server

import org.neo4j.server.CommunityEntryPoint

/**
  * Created by bluejoe on 2019/7/26.
  */
object GraiphServerEntryPoint {

  def main(args: Array[String]) = {
    GraiphServer.touch;
    CommunityEntryPoint.main(args)
  };

  def start(args: Array[String]) = CommunityEntryPoint.start(_);

  def stop(args: Array[String]) = CommunityEntryPoint.stop(_);
}
