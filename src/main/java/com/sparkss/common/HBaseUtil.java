package com.sparkss.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

public class HBaseUtil {
    private static Connection connection = null;

    /**
     * 初始化hbase的连接
     *
     * @throws IOException
     */
    private static void initConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            Configuration conf = HBaseConfiguration.create();
//            conf.set("hbase.zookeeper.quorum", "192.168.21.177,192.168.21.178,192.168.21.179");
//            conf.set("hbase.zookeeper.quorum", "10.20.3.177,10.20.3.178,10.20.3.179");
//            conf.set("hbase.zookeeper.quorum", "117.51.141.24,117.51.142.225");
           // System.out.println(ConfigUtil.getConfig("hbase_connect"));
            conf.set("hbase.zookeeper.quorum", "node01,node02,node03");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            connection = ConnectionFactory.createConnection(conf);
        }
    }

    /**
     * 获得连接
     *
     * @return
     * @throws IOException
     */
    public static Connection getConnection() throws IOException {
        if (connection == null || connection.isClosed()) {
            initConnection();
        }
        //连接可用直接返回连接
        return connection;
    }

    /**
     * 创建表
     *
     * @param tableNameString
     * @param columnFamily
     * @throws IOException
     */
    public static void createTable(Connection connection, String tableNameString, String columnFamily) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf(tableNameString); //d2h (data to HBase)
        HTableDescriptor table = new HTableDescriptor(tableName);
        HColumnDescriptor family = new HColumnDescriptor(columnFamily);
        table.addFamily(family);
        //判断表是否已经存在

        if (!admin.tableExists(tableName)) {
            admin.createTable(table);
        }else{
            //如果表已经存在了，判断列族是否存在
            Table table1 = connection.getTable(TableName.valueOf(tableNameString));
            HTableDescriptor tableDescriptor = table1.getTableDescriptor();
            HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
            for (HColumnDescriptor hColumnDescriptor : columnFamilies) {
                String nameAsString = hColumnDescriptor.getNameAsString();
                if(!columnFamily.equals(nameAsString)){
                    HColumnDescriptor myColumnFamily = new HColumnDescriptor(columnFamily);
                    admin.modifyColumn(TableName.valueOf(tableNameString),myColumnFamily);
                }
            }
        }
        admin.close();
        connection.close();

    }

    /**
     * 判断hbase的表是否存在
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    public static boolean tableExists(String tableName) throws Exception {
        if (connection == null || connection.isClosed()) {
            initConnection();
        }
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            return true;
        }
        return false;
    }

    public static Table getTable(String tableName)throws Exception{
        Connection connection = getConnection();
//        Admin admin = connection.getAdmin();
//        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        return connection.getTable(TableName.valueOf(tableName));
    }

    public static void savePuts(List<Put> putList,String tableName, String family) throws Exception {
        if (!tableExists(tableName)) {
            HBaseUtil.createTable(getConnection(), tableName, family);
        }
        Table table = HBaseUtil.getTable(tableName);
        table.put(putList);
        table.close();
    }
    /**
     * 获取插入HBase的操作put
     * @param rowKeyString
     * @param familyName
     * @param columnName
     * @param columnValue
     * @return
     */
    public static Put createPut(String rowKeyString, byte[] familyName, String columnName, String columnValue) {
        byte[] rowKey = rowKeyString.getBytes();
        Put put = new Put(rowKey);
        put.addColumn(familyName, columnName.getBytes(), columnValue.getBytes());
        return put;
    }
    /**
     * 获取插入HBase的操作put
     * @param rowKeyString
     * @param familyName
     * @param columns      列
     * @return
     */
    public static Put createPut(String rowKeyString, byte[] familyName, Map<String, String> columns) {
        byte[] rowKey = rowKeyString.getBytes();
        Put put = new Put(rowKey);

        for (Map.Entry<String, String> entry : columns.entrySet()) {
            put.addColumn(familyName, entry.getKey().getBytes(), entry.getValue().getBytes());
        }

        return put;
    }
    /**
     * 打印HBase查询结果
     *
     * @param result
     */
    public static void print(Result result) {
        //result是个四元组<行键，列族，列(标记符)，值>
        byte[] row = result.getRow(); //行键
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> familyEntry : map.entrySet()) {
            byte[] familyBytes = familyEntry.getKey(); //列族
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entry : familyEntry.getValue().entrySet()) {
                byte[] column = entry.getKey(); //列
                for (Map.Entry<Long, byte[]> longEntry : entry.getValue().entrySet()) {
                    Long time = longEntry.getKey(); //时间戳
                    byte[] value = longEntry.getValue(); //值
                    System.out.println(String.format("行键rowKey=%s,列族columnFamily=%s,列column=%s,时间戳timestamp=%d,值value=%s", new String(row), new String(familyBytes), new String(column), time, new String(value)));
                }
            }
        }

    }

    public static void main(String[] args) throws IOException {
        Connection connection = getConnection();
        Admin admin1 = connection.getAdmin();

        initConnection();
        Admin admin = HBaseUtil.connection.getAdmin();
    }


}
