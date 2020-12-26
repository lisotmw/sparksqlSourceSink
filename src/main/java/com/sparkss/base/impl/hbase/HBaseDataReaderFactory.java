package com.sparkss.base.impl.hbase;

import com.sparkss.base.consume.ConsumerMgr;
import com.sparkss.base.interf.reuse.Reusable;
import com.sparkss.base.interf.reuse.ReuseDataReaderFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReader;

/**
 * @Author $ zho.li
 * @Date 2020/12/22 17:14
 **/
public class HBaseDataReaderFactory implements ReuseDataReaderFactory<Row> {
    private long flowId;
    public HBaseDataReaderFactory(long flowId){
        this.flowId = flowId;
    }

    @Override
    public DataReader createDataReader() {
        // 延迟初始化 DataReader
        ((Reusable)ConsumerMgr.CONSUMING.getConsumer(flowId).getReaderMgr().getDataReader(flowId)).delayInit();
        return ConsumerMgr.CONSUMING.getConsumer(flowId).getReaderMgr().getDataReader(flowId);
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
