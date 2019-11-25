package cn.pandadb.network.internal.message

/**
  * Created by bluejoe on 2019/11/25.
  */
trait InternalRpcMessage {

}

case class AuthenticationRequest() extends InternalRpcMessage{

}

case class AuthenticationResponse() extends InternalRpcMessage{

}