package com.sparkss.base.consume;

import com.sparkss.base.FlowBean;
import com.sparkss.base.interf.mgr.DSReaderMgr;
import com.sparkss.base.interf.mgr.DSWriterMgr;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 11:13
 **/
public class ConsumObj {
    private FlowBean flowBean;
    private DSWriterMgr writerMgr;
    private DSReaderMgr readerMgr;

    public FlowBean getFlowBean() {
        return flowBean;
    }

    public void setFlowBean(FlowBean flowBean) {
        this.flowBean = flowBean;
    }

    public DSWriterMgr getWriterMgr() {
        return writerMgr;
    }

    public void setWriterMgr(DSWriterMgr writerMgr) {
        this.writerMgr = writerMgr;
    }

    public DSReaderMgr getReaderMgr() {
        return readerMgr;
    }

    public void setReaderMgr(DSReaderMgr readerMgr) {
        this.readerMgr = readerMgr;
    }
}
