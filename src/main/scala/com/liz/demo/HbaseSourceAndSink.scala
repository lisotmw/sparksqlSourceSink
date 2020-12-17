package com.liz.demo

import java.util
import java.util.{Optional, UUID}

import com.liz.utils.HbaseTools
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory, DataSourceReader}
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.JavaConverters.asScalaIteratorConverter

object HbaseSourceAndSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .getOrCreate()
    val df: DataFrame = spark.read
      .format("com.travel.programApp.HBaseSource")
      .option("hbase.table.name", "spark_hbase_sql")
      .option("schema", "`name` STRING, `score` STRING")
      .option("cf.cc", "cf:name,cf:score")
      .load()
    df.explain()
    df.createOrReplaceTempView("sparkHBaseSQL")
    val frame: DataFrame = spark.sql("select * from sparkHBaseSQL where score > 60")

    frame.write.format("com.travel.programApp.HBaseSource")
      .mode(SaveMode.Overwrite)
      .option("hbase.table.name","spark_hbase_write")
      .save()



  }
}

/**
 * circle1
 */
class HBaseSource extends DataSourceV2 with ReadSupport with WriteSupport{
  override def createReader(options: DataSourceOptions): DataSourceReader = {

    val tableName: String = options.get("hbase.table.name").get()

    val columnFamily: String = options.get("cf.cc").get()

    val schema: String = options.get("schema").get()

    new HbaseDataSourceReader(tableName,columnFamily,schema)
  }

  override def createWriter(jobId: String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions)
  : Optional[DataSourceWriter] = {
    Optional.of(new HbaseDataSourceWriter)
  }
}

/**
 * circle2
 * @param tableName
 * @param columnFamily
 * @param schema
 */
class HbaseDataSourceReader(tableName: String, columnFamily: String, schema: String) extends DataSourceReader{
  // 构造字段的个数以及类型
  private val structType: StructType = StructType.fromDDL(schema)
  override def readSchema(): StructType = {
    structType
  }

  override def createDataReaderFactories(): util.List[DataReaderFactory[Row]] = {
    import collection.JavaConverters._
    Seq(
      new HBaseReaderFactory(tableName,columnFamily).asInstanceOf[DataReaderFactory[Row]]
    ).asJava
  }
}

/**
 * circle3
 * @param tableName
 * @param columnFamily
 */
class HBaseReaderFactory(tableName: String, columnFamily: String) extends DataReaderFactory[Row] {
  override def createDataReader(): DataReader[Row] = {
    new HBaseDataReader(tableName,columnFamily)
  }
}

/**
 * circle4
 * @param tableName
 * @param columnFamily
 */
class HBaseDataReader(tableName: String, columnFamily: String) extends DataReader[Row]{

  var conn:Connection = null
  val data:Iterator[Seq[AnyRef]] = getIterator

  def getIterator: Iterator[Seq[AnyRef]] ={
    conn= HbaseTools.getHbaseConn
    val table: Table = conn.getTable(TableName.valueOf(tableName))

    val scanner: ResultScanner = table.getScanner(new Scan())
    val iterator: Iterator[Seq[String]] = scanner.iterator().asScala.map(eachResult => {
      val name: String = Bytes.toString(eachResult.getValue("cf".getBytes, "name".getBytes))
      val score: String = Bytes.toString(eachResult.getValue("cf".getBytes, "score".getBytes))
      Seq(name, score)
    })
    iterator
  }

  override def next(): Boolean = {
    data.hasNext
  }

  override def get(): Row = {
    val seq: Seq[AnyRef] = data.next()
    Row.fromSeq(seq)
  }

  override def close(): Unit = {
    conn.close()
  }
}

class HbaseDataSourceWriter extends DataSourceWriter{
  override def createWriterFactory(): DataWriterFactory[Row] = {
    new HBaseDataWriterFactory
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}

  override def abort(messages: Array[WriterCommitMessage]): Unit = {}
}

class HBaseDataWriterFactory extends DataWriterFactory[Row]{
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new HBaseDataWriter
  }
}

class HBaseDataWriter extends DataWriter[Row]{
  private val conn: Connection = HbaseTools.getHbaseConn
  private val table: Table = conn.getTable(TableName.valueOf("spark_hbase_write"))

  override def write(record: Row): Unit = {
    val name: String = record.getString(0)
    val score: String = record.getString(1)
    val put: Put = new Put(UUID.randomUUID().toString.replace("-", "").getBytes)
    val fam: String = "cf"
    put.addColumn(fam.getBytes,"name".getBytes(),name.getBytes())

    put.addColumn(fam.getBytes,"score".getBytes(),score.getBytes())

    table.put(put)
  }

  override def commit(): WriterCommitMessage = {
    table.close()
    conn.close()
    null
  }

  override def abort(): Unit = {}
}