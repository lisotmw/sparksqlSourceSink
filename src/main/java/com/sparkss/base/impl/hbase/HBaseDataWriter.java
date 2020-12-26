package com.sparkss.base.impl.hbase;

import com.sparkss.base.interf.reuse.ReuseDataWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.io.IOException;

/**
 * @Author $ zho.li
 * @Date 2020/12/23 17:45
 **/
public class HBaseDataWriter implements ReuseDataWriter<Row> {
    private long flowId;
    public HBaseDataWriter(long flowId){
        this.flowId = flowId;
    }
    @Override
    public void write(Row record) throws IOException {

    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        return null;
    }

    @Override
    public void abort() throws IOException {

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
