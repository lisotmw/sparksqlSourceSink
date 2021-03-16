package com.sparkss.base.consume;

import com.sparkss.base.interf.mgr.DSReaderMgr;
import com.sparkss.base.interf.mgr.DSWriterMgr;
import com.sparkss.base.pool.PoolMgr;
import com.sparkss.base.pool.ReaderObjPool;
import com.sparkss.base.pool.WriterObjPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 每次查询的临时缓存管理，包含每次查询的特有数据（sql语句，过滤的字段等），查询需要的 datasourceV2 对象组
 * （org.apache.spark.sql.sources.v2 包下的实现对象，这里直接从对象池里取），每次数据操作完成会自动清理。
 * TODO
 * 发生异常会导致消费者管理类中的对象不能正常被对象池回收，怎么处理呢。。。
 * @Author $ zho.li
 * @Date 2020/12/25 11:11
 **/
public enum ConsumerMgr {
    CONSUMING(),;
    private Map<Long, ConsumerObj> consumingMap = new ConcurrentHashMap<>();

    public void submit(long flowId, ConsumerObj consumerObj){
        if (!consumingMap.containsKey(flowId)){
            consumingMap.put(flowId, consumerObj);
        }
    }

    public ConsumerObj getConsumer(long flowId){
        ConsumerObj consumer = null;
        if (consumingMap.containsKey(flowId)){
            consumer = consumingMap.get(flowId);
        }
        return consumer;
    }

    /**
     * 通过 flowId 移除已经消费的对象，相关读写对象归还对象池
     * @param flowId
     */
    public void remove(long flowId){
        if (consumingMap.containsKey(flowId)){
            ConsumerObj consumed = consumingMap.get(flowId);
            DSReaderMgr readerMgr = consumed.getReaderMgr();
            if (readerMgr != null){
                // 对象池回收
                Class<? extends ReaderObjPool> readerPoolClz
                        = consumed.getFlowBean().getReaderPoolClz();
                PoolMgr.POOL.getReaderPool(readerPoolClz).reBack(readerMgr);
            }
            DSWriterMgr writerMgr = consumed.getWriterMgr();
            if (writerMgr != null){
                // 对象池回收
                Class<? extends WriterObjPool> writerPoolClz
                        = consumed.getFlowBean().getWriterPoolClz();
                PoolMgr.POOL.getWriterPool(writerPoolClz).reBack(writerMgr);
            }
            // 最后才移除
            consumingMap.remove(flowId);
        }
    }

}
