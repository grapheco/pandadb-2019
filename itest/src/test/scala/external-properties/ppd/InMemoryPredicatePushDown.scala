package ppd

import org.junit.Before
import cn.pandadb.externalprops.InMemoryPropertyNodeStore

class InMemoryPredicatePushDown extends QueryCase {

  @Before
  def init(): Unit = {
    buildDB(InMemoryPropertyNodeStore)
  }

}