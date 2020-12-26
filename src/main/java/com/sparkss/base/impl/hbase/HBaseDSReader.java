package com.sparkss.base.impl.hbase;

import com.sparkss.base.interf.mgr.DSReaderMgr;
import com.sparkss.base.interf.reuse.Reusable;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

/**
 * Hbase 池化资源持有的 Reader 可复用实例
 * 优化：考虑使用数组一次性初始化3个对象，达到3个对象具有连续的内存地址，从而达到 程序的局部性原理 优化
 * @Author $ zho.li
 * @Date 2020/12/25 6:57
 **/
public class HBaseDSReader implements DSReaderMgr {

    private long flowId = 0;
    private Reusable[] reusables = new Reusable[3];
    public HBaseDSReader(){
        reusables[0] = new HBaseDataSourceReader(flowId);
        reusables[1] = new HBaseDataReaderFactory (flowId);
        reusables[2] = new HBaseDataReader(flowId);
    }
    @Override
    public DataSourceReader getDataSourceReader(long flowId) {
        return (DataSourceReader)reusables[0];
    }

    @Override
    public DataReaderFactory getDataReaderFactory(long flowId) {
        return (DataReaderFactory)reusables[1];
    }

    @Override
    public DataReader getDataReader(long flowId) {
        return (DataReader)reusables[2];
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
