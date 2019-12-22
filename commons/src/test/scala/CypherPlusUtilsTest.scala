import cn.pandadb.cypherplus.utils.CypherPlusUtils
import org.junit.runners.MethodSorters
import org.junit.{Assert, FixMethodOrder, Test}

import scala.collection.mutable.ListBuffer

/**
  * @Author: Airzihao
  * @Description:
  * @Date: Created at 17:19 2019/12/7
  * @Modified By:
  */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class CypherPlusUtilsTest {
  var writeStatements: ListBuffer[String] = new ListBuffer[String]
  var notWriteStatements: ListBuffer[String] = new ListBuffer[String]

  notWriteStatements.append("EXPlain Create(n:Test)")
  notWriteStatements.append("Explain Match(n:T) Delete n;")
  notWriteStatements.append("expLaIN Match(n) set n.name='panda'")
  notWriteStatements.append("Match(n) \n with n  \n Return n.name")

  writeStatements.append("Create(n:Test{prop:'prop'})")
  writeStatements.append("Merge(n:T{name:'panda'})")
  writeStatements.append("Match(n:Test) sET n.prop=123")
  writeStatements.append("Match(n) Where n.prop=123     DeLETe n")

  @Test
  def test1(): Unit = {
    writeStatements.toList.foreach(statement => {
      if (!CypherPlusUtils.isWriteStatement(statement)) {
        // scalastyle:off
        println(s"error: ${statement} judged as a not-write statement.")
      }
      Assert.assertEquals(true, CypherPlusUtils.isWriteStatement(statement))
    })
  }

  @Test
  def test2(): Unit = {
    notWriteStatements.toList.foreach(statement => {
      if (CypherPlusUtils.isWriteStatement(statement)) {
        // scalastyle:off
        println(s"error: ${statement} judged as a write statement.")
      }
      Assert.assertEquals(false, CypherPlusUtils.isWriteStatement(statement))
    })
  }

}
