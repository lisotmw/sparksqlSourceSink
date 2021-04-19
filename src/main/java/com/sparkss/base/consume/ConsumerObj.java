package com.sparkss.base.consume;

import com.sparkss.base.FlowBean;
import com.sparkss.base.interf.mgr.DSReaderMgr;
import com.sparkss.base.interf.mgr.DSWriterMgr;

/**
 * 每个 flowId 对应的消费者缓存对象：
 * FlowBean         为每次查询的特殊数据（每次查询不同的数据源，字段及类型等）
 * DSWriterMgr      一组缓存的写连接对象
 * DSReaderMgr      一组缓存的读连接对象
 * @Author $ zho.li
 * @Date 2020/12/25 11:13
 **/
public class ConsumerObj {
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
