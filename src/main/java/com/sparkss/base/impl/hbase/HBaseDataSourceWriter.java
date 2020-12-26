package com.sparkss.base.impl.hbase;

import com.sparkss.base.interf.reuse.ReuseDataSourceWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

/**
 * @Author $ zho.li
 * @Date 2020/12/23 17:40
 **/
public class HBaseDataSourceWriter implements ReuseDataSourceWriter {
    private long flowId;
    public HBaseDataSourceWriter(long flowId){
        this.flowId = flowId;
    }
    @Override
    public DataWriterFactory<Row> createWriterFactory() {
        return null;
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {

    }

    @Override
    public void abort(WriterCommitMessage[] messages) {

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
