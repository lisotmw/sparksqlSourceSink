package com.sparkss.base;

import com.sparkss.base.consume.ConsumObj;
import com.sparkss.base.consume.ConsumerMgr;
import com.sparkss.base.exception.SparkParamException;
import com.sparkss.base.inst.DataSourceInstance;
import com.sparkss.base.interf.mgr.DSReaderMgr;
import com.sparkss.base.keys.SparkOptionKey;
import com.sparkss.common.RandomUtil;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

/**
 * 顶层父类，实现了sparksql 自定义数据源需要的 DataSourceV2，
 * 同时支持读写
 * todo：
 * 1.代理 + 对象池 + clone
 * 2.FlowBean对象统一管理维护一个<id,FlowBean>，类层级之间传递id，
 * 3.在枚举层实现通用的检测接口
 *  构建第二层 DataSourceReader的时候，初始化所有关联对象，后期复用直接从对象池里取
 * @Author $ zho.li
 * @Date 2020/12/18 9:45
 **/
public class BaseDataSourceV implements DataSourceV2, ReadSupport, WriteSupport {

    public BaseDataSourceV(){
    }

    @Override
    public DataSourceReader createReader(DataSourceOptions options) {
        ConsumObj consumObj = new ConsumObj();
        FlowBean flowBean = new FlowBean();
        flowBean.setOptions(options);

        String dataBaseMode = options.get(SparkOptionKey.DATA_BASE_MODE).get();
        flowBean.setDataBaseModeMark(dataBaseMode);

        DataSourceInstance instance = DataSourceInstance.getInstance(dataBaseMode);
        if (instance == null){
            throw new SparkParamException("未设置 DataBaseMode，或设置的 DataBaseMode 找不到实例");
        }
        if (!instance.check(options)){
            throw new SparkParamException("spark option 中缺少相关数据库读写的必要参数");
        }
        // 设置读写对象池 class 对象
        flowBean.setReaderPoolClz(instance.getReaderPoolClz());
        flowBean.setWriterPoolClz(instance.getWriterPoolClz());

        long flowId = RandomUtil.generateRandomNumber(15);
        DSReaderMgr dsReaderMgr = instance.getDSReaderMgr(flowId);

        if(dsReaderMgr == null){

        }
        consumObj.setFlowBean(flowBean);
        consumObj.setReaderMgr(dsReaderMgr);
        ConsumerMgr.CONSUMING.submit(flowId,consumObj);
        return dsReaderMgr.getDataSourceReader(flowId);
    }

    @Override
    public Optional<DataSourceWriter> createWriter(String jobId,
                                                   StructType schema,
                                                   SaveMode mode,
                                                   DataSourceOptions options) {
        return null;
    }

}
