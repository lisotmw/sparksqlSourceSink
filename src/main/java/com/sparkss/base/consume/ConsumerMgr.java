package com.sparkss.base.consume;

import com.sparkss.base.interf.mgr.DSReaderMgr;
import com.sparkss.base.interf.mgr.DSWriterMgr;
import com.sparkss.base.pool.PoolMgr;
import com.sparkss.base.pool.ReaderObjPool;
import com.sparkss.base.pool.WriterObjPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TODO
 * 发生异常会导致消费者管理类中的对象不能正常被对象池回收，怎么处理呢。。。
 * @Author $ zho.li
 * @Date 2020/12/25 11:11
 **/
public enum ConsumerMgr {
    CONSUMING(),;
    private Map<Long,ConsumObj> consumingMap = new ConcurrentHashMap<>();

    public void submit(long flowId,ConsumObj consumObj){
        if (!consumingMap.containsKey(flowId)){
            consumingMap.put(flowId,consumObj);
        }
    }

    public ConsumObj getConsumer(long flowId){
        ConsumObj consumer = null;
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
            ConsumObj consumed = consumingMap.get(flowId);
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
