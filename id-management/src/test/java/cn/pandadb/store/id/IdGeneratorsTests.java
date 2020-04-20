package cn.pandadb.store.id;

import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class IdGeneratorsTests {

    String zkAddress = "127.0.0.1:2181";
    String saveDir = "/pandadb-test/ids";

    @Test
    public void testForTypes() throws Exception {
        IdGenerators idGenerators = new IdGenerators(zkAddress, saveDir);
        for(StoreType type:StoreType.values()) {
            assertNotNull(idGenerators.idGenerator(type));
        }
    }
}
