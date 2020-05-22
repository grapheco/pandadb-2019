
# IdGeneratorBasedZK
## 2020-04-20
基于curator的DistributedAtomicLong计数器，实现了一个简单的分布式id生成器；
能保证并发场景下生成递增的唯一id; 每次获取一个id都要进行一次同步加锁访问，性能比较差；
后续需要增加参考neo4j相关实现，增加 上限判断、批分配（提升性能）、id回收等功能；
