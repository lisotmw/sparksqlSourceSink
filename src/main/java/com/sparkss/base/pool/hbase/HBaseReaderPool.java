package com.sparkss.base.pool.hbase;

import com.sparkss.base.impl.hbase.HBaseDSReader;
import com.sparkss.base.pool.ReaderObjPool;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 16:18
 **/
public class HBaseReaderPool extends ReaderObjPool<HBaseDSReader> {

    public HBaseReaderPool(HBaseDSReader reader){
        super(reader);
    }

    @Override
    protected int getSize() {
        return 2;
    }
}
