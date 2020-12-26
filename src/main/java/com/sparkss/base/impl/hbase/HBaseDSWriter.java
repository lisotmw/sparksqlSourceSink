package com.sparkss.base.impl.hbase;

import com.sparkss.base.interf.mgr.DSWriterMgr;
import com.sparkss.base.interf.reuse.Reusable;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

/**
 * Hbase 池化资源持有的 Wirter 可复用实例
 * 优化：考虑使用数组一次性初始化3个对象，达到3个对象具有连续的内存地址，从而达到 程序的局部性原理 优化
 *
 * @Author $ zho.li
 * @Date 2020/12/25 6:57
 *
 **/
public class HBaseDSWriter implements DSWriterMgr{

    private long flowId;
    private Reusable[] reusables = new Reusable[3];
    public HBaseDSWriter(){
        reusables[0] = new HBaseDataSourceWriter(flowId);
        reusables[1] = new HBaseDataWriterFactory (flowId);
        reusables[2] = new HBaseDataWriter(flowId);
    }
    @Override
    public DataSourceWriter getDataSourceWriter(long flowId) {
        return ((DataSourceWriter)this.reusables[0]);
    }

    @Override
    public DataWriterFactory getDataWriterFactory(long flowId) {
        return ((DataWriterFactory) this.reusables[1]);
    }

    @Override
    public DataWriter getDataWriter(long flowId) {
        return ((DataWriter) this.reusables[1]);
    }


    @Override
    public void setFlowId(long flowId) {
        this.flowId = flowId;
        for(Reusable reusable : reusables){
            reusable.setFlowId(flowId);
        }
    }

    @Override
    public void delayInit() {

    }

    @Override
    public long getFlowId() {
        return flowId;
    }
}
