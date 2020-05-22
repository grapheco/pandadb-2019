package cn.pandadb.store.id;


import java.io.Closeable;

public interface IdGenerator extends Closeable {

  long nextId() throws Exception;

  void freeId(long id);

  @Override
  void close();

}
