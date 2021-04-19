package com.sparkss.base.interf.reuse;

import com.sparkss.base.consume.ConsumerMgr;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import java.io.IOException;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 13:29
 **/
public abstract class ReuseDataWriter<T> implements DataWriter<T>,Reusable {

    @Override
    public final WriterCommitMessage commit() throws IOException {
        ConsumerMgr.CONSUMING.remove(getFlowId());
        resetFlowId();
        commit0();
        return null;
    }

    @Override
    public final void abort() throws IOException {
        ConsumerMgr.CONSUMING.remove(getFlowId());
        resetFlowId();
        abort0();
    }

    public abstract WriterCommitMessage commit0() throws IOException;

    public abstract void abort0() throws IOException;


    }
