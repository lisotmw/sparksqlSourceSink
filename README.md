sparksqlSource&Sink

spark-sql 自定义source and sink，整合各种常规数据库 hbase，mysql，redis
参考：
https://blog.csdn.net/zjerryj/article/details/84922369
https://spark.apache.org/docs/2.3.2/api/java/index.html

这是一个小demo，旨在对 spark 2.3 datasource.V2 版本提供自定义数据源方案做封装。

原方案中每次读取操作需要新建多个 中间对象，本项目中利用 java juc 包中 semaphore 共
享状态变量 （多个线程可同时访问临界区）的原理 对读取数据源时的 中间对象 做池化处理
(基类实现为 com.sparkss.base.pool.ObjectPool)，实现了对象重用， 减少了频繁对
象创建和频繁 gc 的开销。

借鉴 disruptor 程序局部性优化思想，对一种数据源的一组中间对象做内存连续性处理
（实现为 com.sparkss.base.impl.hbase.HBaseDSReader）

针对不同数据源，可使用通用的实现类调用(com.liz.base.BaseDataSourceV)，
只需要在 sparkSQL初始化时指定相应的 option 参数(OptionKey 统一处理
com.sparkss.base.keys.SparkOptionKey)即可；

测试用例：com.sparkss.demo.HbaseSourceAndSink（当前只实现了 hbase）

用法：
读取数据源灵活配置（redis，hbase...），需要支持的各种数据源参考 com.sparkss.base.impl.hbase 包
下 hbase 读数据的实现。

注：
datasource.V2 提供的方案在高版本 spark 中已经弃用。
该项目的初衷为学习初期对 类层次结构设计 和 性能优化的练习，并无商用价值，因此只写了 hbase 读
数据的用例进行测试，测试结果较原始版本性能提审 7%-10%。商用可考虑 高版本 spark 提供的新的优
化方案，也可考虑 flink 提供的自定义数据源方案

