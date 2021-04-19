package com.sparkss.base.impl.hbase;

import com.sparkss.base.consume.ConsumerMgr;
import com.sparkss.base.interf.reuse.ReuseDataWriter;
import com.sparkss.base.log.Logger0;
import com.sparkss.common.HBaseUtil;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @Author $ zho.li
 * @Date 2020/12/23 17:45
 **/
public class HBaseDataWriter extends ReuseDataWriter<Row> implements Logger0 {

    private Connection conn;

    private long flowId;
    public HBaseDataWriter(long flowId){
        this.flowId = flowId;
    }
    @Override
    public void write(Row record) throws IOException {
        conn = HBaseUtil.getConnection();

        try{
            String tableName = ConsumerMgr.CONSUMING.getConsumer(flowId).getFlowBean().getTableName();
            // 列族 列名
            String hbaseCfcc = ConsumerMgr.CONSUMING.getConsumer(flowId).getFlowBean().getHbaseCfCc();
            String rowKey = ConsumerMgr.CONSUMING.getConsumer(flowId).getFlowBean().getRowKey();
            int regions = ConsumerMgr.CONSUMING.getConsumer(flowId).getFlowBean().getHbaseRegions();

            // <列名，列族>
            java.util.Map<String, String> cccfMap = Arrays.stream(hbaseCfcc.split(","))
                    .map(s -> s.split(":"))
                    .collect(Collectors.toMap(a -> a[1], a -> a[0]));

            // 从 schema 中取出 列字段信息
            Stream<String> fieldsStream = cccfMap.keySet().stream();
            // java 数组转成 scala seq
            Seq<String> seqFields = JavaConverters.asScalaIteratorConverter(fieldsStream.iterator()).asScala().toSeq();
            Map<String, String> valuesMap = record.getValuesMap(seqFields);
            // 数据插入 hbase
            updateHaseData(tableName, rowKey, valuesMap, cccfMap, regions);
        }finally {
            conn.close();
        }
    }

    /**
     *
     * @param tableName
     * @param rowKeyStr
     * @param data
     * @param cccfMap
     * @param regions
     */
    private void updateHaseData(String tableName, String rowKeyStr,
                                Map<String, String> data, java.util.Map<String,String> cccfMap,
                                int regions){
        Table table = null;
        try{
            Admin admin = conn.getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))){
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
                HColumnDescriptor familyDescriptor = new HColumnDescriptor(cccfMap.values().stream().findFirst().get());
                hTableDescriptor.addFamily(familyDescriptor);

                admin.createTable(hTableDescriptor);
            }

            table = conn.getTable(TableName.valueOf(tableName));
            String rowKey = getRowKey(rowKeyStr, regions);
            Put put = new Put(Bytes.toBytes(rowKey));
            if (data.size() > 0){
                java.util.Map<String, String> javaMapData = JavaConverters.mapAsJavaMapConverter(data).asJava();
                for(java.util.Map.Entry<String,String> entry : javaMapData.entrySet()){
                    put.addColumn(
                            Bytes.toBytes(cccfMap.get(entry.getKey())),
                            Bytes.toBytes(entry.getKey()),
                            Bytes.toBytes(entry.getValue())
                    );
                }
            }
            table.put(put);
        }catch (IOException e){
            getLogger().error("hbase 获取 admin 失败");
        }finally {
            try{
                table.close();
            }catch (IOException e){}
        }

    }

    /**
     * rowkey 的规则
     * @param str           默认的 rowkey 前缀字符串
     * @param regions       该表的 region 个数
     * @return
     */
    private String getRowKey(String str, int regions){
        int region = (str.hashCode() & Integer.MAX_VALUE) % regions;
        String prefix = StringUtils.leftPad(region + "", 4, "0");
        String suffix = DigestUtils.md5Hex(str).substring(0, 12);
        return prefix + suffix;
    }

    @Override
    public WriterCommitMessage commit0() throws IOException {
        conn.close();
        return null;
    }

    @Override
    public void abort0() throws IOException {

    }

    @Override
    public void setFlowId(long flowId) {
        this.flowId = flowId;
    }

    @Override
    public long getFlowId() {
        return flowId;
    }

    @Override
    public void delayInit() {

    }
}
