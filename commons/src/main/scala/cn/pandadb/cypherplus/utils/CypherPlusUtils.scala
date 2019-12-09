package cn.pandadb.cypherplus.utils

import java.util.Locale

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 14:50 2019/11/27
  * @Modified By:
  */
object CypherPlusUtils {

  def isWriteStatement(cypherStr: String): Boolean = {
    val lowerCypher = cypherStr.toLowerCase(Locale.ROOT)
    if (lowerCypher.contains("explain")) {
      false
    } else if (lowerCypher.contains("create") || lowerCypher.contains("merge") ||
      lowerCypher.contains("set") || lowerCypher.contains("delete")) {
      true
    } else {
      false
    }
  }
}
