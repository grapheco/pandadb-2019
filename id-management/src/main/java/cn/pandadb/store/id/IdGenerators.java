package cn.pandadb.store.id;

import cn.pandadb.store.id.impl.IdGeneratorBasedZK;

import java.io.Closeable;
import java.io.IOException;

public class IdGenerators implements Closeable {
    private final IdGenerator[] types = new IdGenerator[StoreType.values().length];

    public IdGenerators(String zkAddress, String idSaveDir) throws Exception {
        if (!checkZkPath(idSaveDir)) { throw new Exception("idSaveDir is not valid!"); }
        if (!idSaveDir.endsWith("/")) { idSaveDir = idSaveDir + "/"; }

        for ( StoreType type : StoreType.values() ) {
            String savePath = idSaveDir + type.name();
            types[type.ordinal()] = new IdGeneratorBasedZK(zkAddress, savePath);
        }
    }

    private boolean checkZkPath(String path) {
        if (path == null || path.trim().isEmpty()) {
            return false;
        }
        return true;
    }

    public long nextId(StoreType type) throws Exception {
        return idGenerator(type).nextId();
    }

    public IdGenerator idGenerator(StoreType type) {
        return types[type.ordinal()];
    }


    @Override
    public void close() throws IOException {
        for ( StoreType type : StoreType.values() ) {
            types[type.ordinal()].close();
        }
    }
}
