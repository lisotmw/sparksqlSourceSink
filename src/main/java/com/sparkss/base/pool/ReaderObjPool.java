package com.sparkss.base.pool;

import com.sparkss.base.interf.mgr.DSReaderMgr;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 16:06
 **/
public abstract class ReaderObjPool<T extends DSReaderMgr> extends ObjectPool<T> {
    public ReaderObjPool(T dsReaderMgr) {
        super(dsReaderMgr);
    }
}
