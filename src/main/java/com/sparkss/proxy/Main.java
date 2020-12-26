package com.sparkss.proxy;

import com.sparkss.base.FlowBean;
import com.sparkss.base.impl.hbase.HBaseDataSourceReader;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author $ zho.li
 * @Date 2020/12/24 20:48
 **/
public class Main {
    public static void main(String[] args) {
        FlowBean flowBean = new FlowBean();
        flowBean.setJobId("sdfwefaw");
        flowBean.setSchemaStr("`name` STRING,`score` STRING");
        flowBean.setTableName("hbase_spark");
        HBaseDataSourceReader origion = new HBaseDataSourceReader(1);
        int totle = 30000;
        List<HBaseDataSourceReader> one = new ArrayList<>(totle);
        long time1 = System.nanoTime();
        for(int i = 0; i < totle;i++){
//            one.add(new HBaseDataSourceReader(flowBean));

            HBaseDataSourceReader clone = origion.getClone();
            clone.setFlowBean(1);
            one.add(clone);
        }
        long time10 = System.nanoTime();
        List<HBaseDataSourceReader> two = new ArrayList<>(totle);
        long time2 = System.nanoTime();
        for(int i = 0; i < totle;i++){
            two.add(new HBaseDataSourceReader(1));
//            HBaseDataSourceReader clone = origion.getClone();
//            clone.setFlowBean(flowBean);
//            two.add(clone);
        }
        long time3 = System.nanoTime();

        System.out.printf("copy 一个对象%d次耗时%d \n",totle,time10 - time1);
        System.out.printf("new 一个对象%d次耗时%d \n",totle,time3 - time2);
    }
}
