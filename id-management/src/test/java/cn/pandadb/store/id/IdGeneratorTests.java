package cn.pandadb.store.id;

import java.lang.Thread;
import java.util.Arrays;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import cn.pandadb.store.id.impl.IdGeneratorBasedZK;

public class IdGeneratorTests {

  String zkAddress = "127.0.0.1:2181";
  String counterPath = "/pandadb-test/id3";

  @Test
  public void testNextIdForOneThread() throws Exception {
    IdGeneratorBasedZK idGenerator = new IdGeneratorBasedZK(zkAddress, counterPath);
    long highId = idGenerator.getHighestPossibleIdInUse();
    for (int i=0; i<5; i++) {
      assertEquals ((++highId), idGenerator.nextId());
    }
  }

  @Test
  public void testNextIdForMultiThreads() throws Exception {
    IdGeneratorBasedZK idGenerator = new IdGeneratorBasedZK(zkAddress, counterPath);
    long highId = idGenerator.getHighestPossibleIdInUse()+1;

    final int threadsCount = 5;
    final int idCount = 10;

    long[] ids = new long[threadsCount*idCount];
    Thread[] threads = new Thread[threadsCount];

    for (int i=0; i<threadsCount; i++){
      threads[i] = new Thread(new Runnable() {
        private int index = 0;
        public Runnable setIndex(int index) {
          this.index = index;
          return this;
        }
        @Override
        public void run() {
          for(int j=0; j<idCount; j++){
            try {
              ids[index+j] = idGenerator.nextId();
              Thread.sleep(200L);
            } catch (Exception e) {
              e.printStackTrace();
              ids[index+j] = -1;
            }
          }
        }
      }.setIndex(i*idCount));
      threads[i].start();
    }
    for (Thread thread: threads) { thread.join(); }
    System.out.println(Arrays.toString(ids));
    Arrays.sort(ids);
    System.out.println(Arrays.toString(ids));

    for (int i=0; i<threadsCount*idCount; i++) {
      assertEquals (ids[i], highId+i);
    }
  }

}
