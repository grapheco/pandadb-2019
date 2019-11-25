package cn.pandadb.network.internal.message

/**
  * Created by bluejoe on 2019/11/25.
  */
trait InternalRpcRequest {

}

trait InternalRpcResponse {

}

case class AuthenticationRequest() extends InternalRpcRequest{

}

case class AuthenticationResponse() extends InternalRpcResponse{

}