package com.sparkss.base.pool;

import com.sparkss.base.interf.mgr.DSWriterMgr;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 16:08
 **/
public abstract class WriterObjPool<T extends DSWriterMgr> extends ObjectPool<T> {

    public WriterObjPool(T dsWriterMgr) {
        super(dsWriterMgr);
    }
}
