package cn.pandadb.store.id.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;

import cn.pandadb.store.id.IdGenerator;


public class IdGeneratorBasedZK implements IdGenerator {
  private String zkServerAddress;
  private CuratorFramework curator;
  private String highIdSavePath;
  private DistributedAtomicLong highId;

  public IdGeneratorBasedZK(String zkAddress, String highIdSavePath) {
    this.zkServerAddress = zkAddress;
    this.highIdSavePath = highIdSavePath;
    init();
  }

  public boolean init(){
    boolean result = true;

    curator = CuratorFrameworkFactory.newClient(zkServerAddress,
            new ExponentialBackoffRetry(1000, 3));
    try {
      curator.start();
      highId = new DistributedAtomicLong(curator, highIdSavePath, new ExponentialBackoffRetry(1000, 3));
      highId.initialize(0L);
    } catch (Exception e) {
      e.printStackTrace();
      result = false;
    }
    return result;
  }

  private long getHighId() throws Exception {
      return highId.get().postValue();
  }

  public long getHighestPossibleIdInUse() throws Exception {
    return getHighId() - 1;
  }

  @Override
  public long nextId() throws Exception {
    AtomicValue<Long> tmp = highId.add(1L);
    if (tmp.succeeded()) {
      return tmp.postValue() - 1;
    }
    else {
      throw new Exception("Failed to Get nextId()");
    }
  }

  @Override
  public void freeId(long id) {
    return;
  }

  @Override
  public void close() {
    curator.close();
  }
}