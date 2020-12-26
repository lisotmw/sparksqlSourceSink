package com.sparkss.base.pool.hbase;

import com.sparkss.base.impl.hbase.HBaseDSWriter;
import com.sparkss.base.pool.WriterObjPool;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 16:09
 **/
public class HBaseWriterPool extends WriterObjPool<HBaseDSWriter> {

    public HBaseWriterPool(HBaseDSWriter writer){
        super(writer);
    }
    @Override
    protected int getSize() {
        return 2;
    }
}
