package com.sparkss.base.interf.reuse;

import com.sparkss.base.consume.ConsumerMgr;
import org.apache.spark.sql.sources.v2.reader.DataReader;

import java.io.IOException;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 13:26
 **/
public abstract class ReuseDataReader<T> implements DataReader<T>,Reusable {
//    public ReuseDataReader(){}
    @Override
    public final void close() throws IOException {
        ConsumerMgr.CONSUMING.remove(getFlowId());
        resetFlowId();
        close0();
    }

    protected abstract void close0() throws IOException;
}
