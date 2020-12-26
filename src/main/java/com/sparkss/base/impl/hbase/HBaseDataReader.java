package com.sparkss.base.impl.hbase;

import com.sparkss.base.FlowBean;
import com.sparkss.base.consume.ConsumerMgr;
import com.sparkss.base.interf.reuse.ReuseDataReader;
import com.sparkss.base.log.Log;
import com.sparkss.common.HBaseUtil;
import com.sparkss.common.JavaConversion;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.sql.Row;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author $ zho.li
 * @Date 2020/12/22 21:49
 **/
public class HBaseDataReader extends ReuseDataReader<Row> {
    private long flowId;
    private Connection conn;
    private Iterator<Result> iterator;
    public HBaseDataReader(long flowId){
        super();
        this.flowId = flowId;
    }

    private Iterator<Result> getIterator(long flowId){
        try{
            conn = HBaseUtil.getConnection();
            Table table = conn.getTable(TableName.valueOf(getTableName().trim()));
            Scan scan = new Scan();
            String[] cfccs = getHbaseCfCc().split(",");
            for(String cfcc : cfccs){
                String[] familyColumn = cfcc.split(":");
                String columnName = familyColumn[1].trim();
                // 列剪枝
                if (!getRequiredSchemaList().isEmpty() &&
                        getRequiredSchemaList().contains(columnName)){
                    scan.addColumn(familyColumn[0].getBytes(),familyColumn[1].getBytes());
                }
            }
            ResultScanner scanner = table.getScanner(scan);
            return scanner.iterator();
        }catch (IOException e){
            Log.getLogger(this.getClass()).error("获取 hbase 连接异常",e);
            return null;
        }
    }

    @Override
    public boolean next() throws IOException {
        if (iterator == null)
            return false;
        return iterator.hasNext();
    }

    @Override
    public Row get() {
        if (iterator == null)
            return null;
        Result next = iterator.next();
        String[] cfccs = getHbaseCfCc().split(",");
        List<Object> resultL_ = new ArrayList<>(next.size());
        for(String cfcc : cfccs){
            String[] familyColumn = cfcc.split(":");
            String columnName = familyColumn[1];
            if (!getRequiredSchemaList().isEmpty() &&
                    getRequiredSchemaList().contains(columnName)){
                String val = Bytes.toString(next.getValue(familyColumn[0].getBytes(), familyColumn[1].getBytes()));
                resultL_.add(val);
            }
        }
        return JavaConversion.asScala(resultL_);
    }

    private FlowBean getFlowBean(){
        return ConsumerMgr.CONSUMING.getConsumer(flowId).getFlowBean();
    }

    private List<String> getRequiredSchemaList(){
        List<String> requiredSchema = new ArrayList<>();
        FlowBean flowBean = getFlowBean();
        if (flowBean.getRequiredSchemaList() != null){
            requiredSchema = flowBean.getRequiredSchemaList();
        }
        return requiredSchema;
    }

    private String getHbaseCfCc(){
        return getFlowBean().getHbaseCfCc();
    }

    private String getTableName(){
        return getFlowBean().getTableName();
    }

    @Override
    public void setFlowId(long flowId) {
        this.flowId = flowId;
    }

    @Override
    public void delayInit() {
        if (flowId > 0){
            this.iterator = getIterator(flowId);
        }
    }

    @Override
    public long getFlowId() {
        return flowId;
    }

    @Override
    protected void close0() throws IOException {
        if (conn != null){
            conn.close();
        }
    }
}
