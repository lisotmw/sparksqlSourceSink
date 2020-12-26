package com.sparkss.utils

import java.util.regex.Pattern

import com.sparkss.common.{ConfigUtil, Constants, DateUtils, HBaseUtil, JedisUtil}
import com.sparkss.common.HBaseUtil
import com.sparkss.log.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.{FsAction, FsPermission}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.RegionSplitter.HexStringSplit
import org.apache.hadoop.hbase.util.{Base64, Bytes, MD5Hash}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
/**
  * Created by laowang
  */
object HbaseTools extends Logging with Serializable {



  def getHbaseConfiguration(): Configuration = {
    val hconf: Configuration = HBaseConfiguration.create()
    hconf.set("hbase.zookeeper.quorum", "node01,node02,node03")
    hconf.set("hbase.zookeeper.property.clientPort", "2181")
    hconf.setInt("hbase.client.operation.timeout", 3000)
    hconf
  }


  // 对指定的列构造rowKey,采用Hash前缀拼接业务主键的方法
  def rowKeyWithHashPrefix(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val hash_prefix = getHashCode(rkString)
    val rowKey = Bytes.add(Bytes.toBytes(hash_prefix), Bytes.toBytes(rkString))
    rowKey
  }

  // 对指定的列构造rowKey, 采用Md5 前缀拼接业务主键方法，主要目的是建表时采用MD5 前缀进行预分区
  def rowKeyWithMD5Prefix(separator:String,length: Int,column: String*): Array[Byte] = {
    val columns = column.mkString(separator)

    var md5_prefix = MD5Hash.getMD5AsHex(Bytes.toBytes(columns))
    if (length < 8){
      md5_prefix = md5_prefix.substring(0, 8)
    }else if (length >= 8 || length <= 32){
      md5_prefix = md5_prefix.substring(0, length)
    }
    val row = Array(md5_prefix,columns)
    val rowKey = Bytes.toBytes(row.mkString(separator))
    rowKey
  }

  // 对指定的列构造RowKey,采用MD5方法
 /* def rowKeyByMD5(column: String*): Array[Byte] = {
    val rkString = column.mkString("")
    val md5 = MD5Hash.getMD5AsHex(Bytes.toBytes(rkString))
    val rowKey = Bytes.toBytes(md5.substring(0 , 16))
    rowKey
  }*/
  // 直接拼接业务主键构造rowKey
  def rowKey(column:String*):Array[Byte] = Bytes.toBytes(column.mkString(""))

  // Hash 前缀的方法：指定列拼接之后与最大的Short值做 & 运算
  // 目的是预分区，尽量保证数据均匀分布
  private def getHashCode(field: String): Short ={
    (field.hashCode() & 0x7FFF).toShort
  }



  /**
    * @param tablename 表名
    * @param regionNum 预分区数量
    * @param columns 列簇数组
    */
  def createHTableByHexStringSplit(connection: Connection, tablename: String,regionNum: Int, columns: Array[String]): Unit = {
    this.synchronized{
      val hexsplit: HexStringSplit = new HexStringSplit()
      // 预先构建分区，指定分区的start key
      val splitkeys: Array[Array[Byte]] = hexsplit.split(regionNum)
      val admin = connection.getAdmin
      val tableName = TableName.valueOf(tablename)
      if (!admin.tableExists(tableName)) {
        val tableDescriptor = new HTableDescriptor(tableName)
        if (columns != null) {
          columns.foreach(c => {
            val hcd = new HColumnDescriptor(c.getBytes()) //设置列簇
            hcd.setMaxVersions(1)
            hcd.setBloomFilterType(BloomType.ROW)
            hcd.setCompressionType(Algorithm.SNAPPY) //设定数据存储的压缩类型.默认无压缩(NONE)
            tableDescriptor.addFamily(hcd)
          })
        }
        admin.createTable(tableDescriptor,splitkeys)
      }
    }
  }

  /**
    * 删除hdfs下的文件
    * @param url 需要删除的路径
    */
  def delete_hdfspath(url: String) {
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    val path: Path = new Path(url)
    if (hdfs.exists(path)) {
      val filePermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ)
      hdfs.delete(path, true)
    }
  }
  def convertScanToString(scan: Scan):String={
    val proto = ProtobufUtil.toScan(scan)
    return Base64.encodeBytes(proto.toByteArray)
  }









//  val configuration = HBaseConfiguration.create()
//  configuration.set("hbase.zookeeper.quorum",Constants.ZK_IP)
//  val connection = ConnectionFactory.createConnection(configuration)

  import scala.collection.JavaConverters._

  def saveBatchData(chengduListBuffer: ListBuffer[Put], topic: String, family: String) = {
    HBaseUtil.savePuts(chengduListBuffer.asJava,topic,family)
  }

  def getStreamingContextFromHBase(streamingContext: StreamingContext,
                                   kafkaParams: Map[String, Object],
                                   topics: Array[String],
                                   group: String,matchPattern:String): InputDStream[ConsumerRecord[String, String]] = {
    val connection: Connection = getHbaseConn
    val admin: Admin = connection.getAdmin
    println("lizzzzzzzz getting offset...")
    val getOffset:collection.Map[TopicPartition, Long]  = HbaseTools.getOffsetFromHBase(topics,group,connection,admin)
    println("lizzzzzzzz offset: " + getOffset.size)
    val result = if(getOffset.size > 0){
      val consumerStrategy: ConsumerStrategy[String, String] =  ConsumerStrategies.SubscribePattern[String,String](Pattern.compile(matchPattern),kafkaParams,getOffset)
      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,consumerStrategy)
      println("lizzzzzzzz later Stream: ")
      value
    }else{
      val consumerStrategy: ConsumerStrategy[String, String] =  ConsumerStrategies.SubscribePattern[String,String](Pattern.compile(matchPattern),kafkaParams)
      val value: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,consumerStrategy)
      println("lizzzzzzzz firstCreate Stream: ")
      value
    }
    admin.close()
    connection.close()
    println("lizzzzzzzz streaming return..." + result)
    result
  }

  def getOffsetFromHBase(topics: Array[String], group: String, hConn: Connection, admin: Admin): collection.Map[TopicPartition, Long] = {
    if (!admin.tableExists(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))) {
      val chengdu_gps_offset = new HTableDescriptor(TableName.valueOf(Constants.HBASE_OFFSET_STORE_TABLE))
      chengdu_gps_offset.addFamily(new HColumnDescriptor(Constants.HBASE_OFFSET_FAMILY_NAME))
      admin.createTable(chengdu_gps_offset)
      admin.close()
    }
    val table: Table = hConn.getTable(TableName.valueOf(ConfigUtil.getConfig(Constants.HBASE_OFFSET_STORE_TABLE)))
    println("lizzzzzzzz table: " + table)
    val returnVal = new mutable.HashMap[TopicPartition, Long]()
    for (topic <- topics) {
      val rowKey = new Get((group + ":" + topic).getBytes())
      val result: Result = table.get(rowKey)
      val cells: Array[Cell] = result.rawCells()
      for (cell <- cells) {
        println("lizzzzzzzz cell: " + cell)
        //列名  group:topic:partition
        val topicPartition: String = Bytes.toString(CellUtil.cloneQualifier(cell))
        //列值 offset
        val offset: String = Bytes.toString(CellUtil.cloneValue(cell))
        //切割列名，获取 消费组，消费topic，消费partition
        val items: Array[String] = topicPartition.split(":")
        val tp = new TopicPartition(items(1), items(2).toInt)
        println("lizzzzzzzz tp: " + tp)
        returnVal += (tp -> offset.toLong)
      }
    }
    table.close()
    returnVal
  }

  def getHbaseConn: Connection = {
    try{
      val config:Configuration = HBaseConfiguration.create()
//      config.set("hbase.zookeeper.quorum" , GlobalConfigUtils.getProp("hbase.zookeeper.quorum"))
   //   config.set("hbase.master" , GlobalConfigUtils.getProp("hbase.master"))
//      config.set("hbase.zookeeper.property.clientPort" , GlobalConfigUtils.getProp("hbase.zookeeper.property.clientPort"))

      config.set("hbase.zookeeper.quorum", "node01,node02,node03")
      config.set("hbase.zookeeper.property.clientPort", "2181")
      //      config.set("hbase.rpc.timeout" , GlobalConfigUtils.rpcTimeout)
      //      config.set("hbase.client.operator.timeout" , GlobalConfigUtils.operatorTimeout)
      //      config.set("hbase.client.scanner.timeout.period" , GlobalConfigUtils.scannTimeout)
      //      config.set("hbase.client.ipc.pool.size","200");
      val connection = ConnectionFactory.createConnection(config)
      connection

    }catch{
      case exception: Exception =>
        error(exception.getMessage)
        error("HBase获取连接失败")
        null
    }
  }

  def closeConn(conn : Connection) = {
    try {
      if (!conn.isClosed){
        conn.close()
      }
    }catch {
      case exception: Exception =>
        error("HBase连接关闭失败")
        exception.printStackTrace()
    }
  }

}
