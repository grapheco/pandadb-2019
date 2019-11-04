package cn.graiph.cnode

/**
  * Created by bluejoe on 2019/11/4.
  */
case class NodeAddress(host: String, port: Int) {

}

trait GNodeList {
  def getReadNodes(): Array[NodeAddress];

  def getWriteNodes(): Array[NodeAddress];
}

trait GNodeSelector {
  def chooseReadNode(): NodeAddress;

  def chooseWriteNode(): NodeAddress;
}