package com.sparkss.base.interf;

import com.sparkss.base.interf.mgr.DSReaderMgr;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 7:02
 **/
public interface DSReader {
    DSReaderMgr getDSReaderMgr(long flowId);
}
