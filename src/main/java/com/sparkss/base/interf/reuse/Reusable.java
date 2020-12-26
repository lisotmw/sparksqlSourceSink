package com.sparkss.base.interf.reuse;

/**
 * 可复用类接口
 * @Author $ zho.li
 * @Date 2020/12/25 13:16
 **/
public interface Reusable {
    /** 每次读取或写入数据库，为一个flow，有单独 flowId 与之对应，通过 flowId 可以获取对象池复用对象 */
    void setFlowId(long flowId);

    /** 任务结束，重置对象中的 flowId */
    default void resetFlowId(){
        setFlowId(0L);
    }

    long getFlowId();

    /** 延迟初始化(特定对象需要 FlowBean 相关数据有了之后才能初始化) */
    void delayInit();
}
