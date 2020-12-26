package com.sparkss.demo

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sparkss.common.JedisUtil
import com.sparkss.utils.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import redis.clients.jedis.Jedis

/**
 *
 * @Author $ zho.li
 * @Date 2020/12/19 10:03
 *
 * */
object RedisSourceAndSink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
    val df: DataFrame = spark.read
      .format("com.liz.demo.RedisSource")
      .option("hbase.table.name", "spark_hbase_sql")
      .option("schema", "`name` STRING, `score` STRING")
      .option("cf.cc", "cf:name,cf:score")
      .load()
    df.explain()
    df.createOrReplaceTempView("sparkRedisSQL")
    df.select()
    val frame: DataFrame = spark.sql("select * from sparkHBaseSQL where score > 60")

    frame.write.format("com.liz.demo.HBaseSource")
      .mode(SaveMode.Overwrite)
      .option("hbase.table.name","spark_redis_write")
      .save()
  }
}

class RedisDataReader extends DataReader[Row]{
  var conn: Jedis = null;

  val data:Iterator[Seq[AnyRef]] = getIterator

  def getIterator: Iterator[Seq[AnyRef]] ={
    conn= JedisUtil.getJedis
    val jsonStr: String = conn.get("")
    val nObject: JSONObject = JSON.parseObject(jsonStr)
//    val table: Table = conn.getTable(TableName.valueOf(tableName))
//
//    val scanner: ResultScanner = table.getScanner(new Scan())
//    val iterator: Iterator[Seq[String]] = scanner.iterator().asScala.map(eachResult => {
//      val name: String = Bytes.toString(eachResult.getValue("cf".getBytes, "name".getBytes))
//      val score: String = Bytes.toString(eachResult.getValue("cf".getBytes, "score".getBytes))
//      Seq(name, score)
//    })
//    iterator
    null
  }


  override def next(): Boolean = ???

  override def get(): Row = ???

  override def close(): Unit = ???
}