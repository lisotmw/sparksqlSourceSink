package com.sparkss.base.interf;

import com.sparkss.base.interf.mgr.DSWriterMgr;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 7:03
 **/
public interface DSWriter {
    DSWriterMgr getDSWriterMgr(long flowId);
}
