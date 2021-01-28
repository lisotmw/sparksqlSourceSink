sparksqlSource&Sink
spark-sql 自定义source and sink，整合各种常规数据库 hbase，mysql，redis
参考：https://blog.csdn.net/zjerryj/article/details/84922369

特色：1.针对不同数据源，可使用通用的实现类调用，只需要在 sparkSQL初始化时指定相应的 option 参数即可；
     2.使用池化资源的方式重用对象，避免频繁 gc 带来的性能资源开销；
     3.一次查询的常用对象组使用一个数组存放；达到内存连续的效果（借鉴disruptor 的程序局部性优化思想）

基础功能点：
1.读取数据源
    1.1.列剪枝
    1.2.自定义过滤算子（这个有点难度，暂时跳过）
    1.3.读取数据源灵活配置（redis，hbase...）
    1.4.最后一层接口 DataReader 需要定义iterator()获取抽象的迭代器
    1.5.字段名和类型信息，定义一个类来疯转，传递到最低层DataReader
2.数据输出
    2.1.提交失败回滚逻辑，要支持自定义
    2.2.字段名和类型信息，定义一个类来疯转，传递到最低层DataWriter
