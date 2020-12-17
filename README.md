sparksqlSource&Sink
spark-sql 自定义source and sink，整合各种常规数据库 hbase，mysql，redis
参考：https://blog.csdn.net/zjerryj/article/details/84922369

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
