package com.sparkss.base.impl.hbase;

import com.sparkss.base.interf.reuse.ReuseDataWriterFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.writer.DataWriter;

/**
 * @Author $ zho.li
 * @Date 2020/12/23 17:42
 **/
public class HBaseDataWriterFactory implements ReuseDataWriterFactory<Row> {

    private long flowId;

    public HBaseDataWriterFactory(long flowId){
        this.flowId = flowId;
    }

    @Override
    public DataWriter<Row> createDataWriter(int partitionId, int attemptNumber) {
        return null;
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
