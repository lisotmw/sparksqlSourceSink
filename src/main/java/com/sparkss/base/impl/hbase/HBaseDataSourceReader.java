package com.sparkss.base.impl.hbase;

import com.sparkss.base.FlowBean;
import com.sparkss.base.consume.ConsumerMgr;
import com.sparkss.base.interf.reuse.ReuseDataSourceReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.Iterator;

import java.util.ArrayList;
import java.util.List;

/**
 * 基于 HBase 的 DataSourceReader 实现，支持列剪枝
 * TODO 可以通过代理 + 对象池 + clone 模式简化对象的创建，优化性能
 * @Author $ zho.li
 * @Date 2020/12/21 17:21
 **/
public class HBaseDataSourceReader implements ReuseDataSourceReader, SupportsPushDownRequiredColumns,Cloneable {

    private long flowId;

    private StructType requiredSchema;

    public HBaseDataSourceReader(long flowId){
        this.flowId = flowId;
    }

    private FlowBean getFlowBean(){
        return ConsumerMgr.CONSUMING.getConsumer(flowId).getFlowBean();
    }

    private String getSchemaStr(){
        return getFlowBean().getSchemaStr();
    }

    public void setFlowBean(long flowId){
        this.flowId = flowId;
    }

    /**
     *spark.read().load().select("...") 中的 schema 从这里传进来
     * @param requiredSchema
     */
    @Override
    public void pruneColumns(StructType requiredSchema) {
        this.requiredSchema = requiredSchema;
    }

    @Override
    public StructType readSchema() {
        StructType result;
        if (requiredSchema != null)
            result = requiredSchema;
        else
            result = StructType.fromDDL(getSchemaStr());
        getFlowBean().setSchema(result);
        return result;
    }

    @Override
    public List<DataReaderFactory<Row>> createDataReaderFactories() {
        List<DataReaderFactory<Row>> factoryL_ = new ArrayList<>();
        Iterator<StructField> iterator = requiredSchema.iterator();

        List<String> requireSchemaL_ = new ArrayList<>();

        while(iterator.hasNext()){
            StructField next = iterator.next();
            String name = next.name();
            requireSchemaL_.add(name);
        }
        FlowBean flowBean = getFlowBean();
        flowBean.setRequiredSchemaList(requireSchemaL_);
        factoryL_.add(ConsumerMgr.CONSUMING.getConsumer(flowId).getReaderMgr().getDataReaderFactory(flowId));
        return factoryL_;
    }

    public HBaseDataSourceReader getClone(){
        HBaseDataSourceReader copy = null;
        try{
            copy = (HBaseDataSourceReader) clone();
        }catch (CloneNotSupportedException e){
            e.printStackTrace();
        }
        return copy;
    }

    @Override
    public void setFlowId(long flowId) {
        this.flowId = flowId;
    }

    @Override
    public long getFlowId() {
        return flowId;
    }

    @Override
    public void delayInit() {

    }
}
