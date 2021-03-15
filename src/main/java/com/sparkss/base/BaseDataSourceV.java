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
 * todo：
 * 1.所有 DataSourceV2 相关的中间对象，使用对象池，避免频繁创建 和 gc
 * 2.FlowBean对象(每次查询读取数据时的状态数据，例如查询时的 sql，或是需要过滤的字段信息等)
 * 统一管理维护一个<id,FlowBean>（com.sparkss.base.consume.ConsumerMgr.CONSUMING），类层级之间传递id，
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

        // 随机生成一个 flowId
        long flowId = RandomUtil.generateRandomNumber(15);
        // 从对象池读取 ReaderMgr 并设置 flowId
        DSReaderMgr dsReaderMgr = instance.getDSReaderMgr(flowId);

        consumObj.setFlowBean(flowBean);
        consumObj.setReaderMgr(dsReaderMgr);
        // 每次查询作为一次记录放入 临时缓存
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
