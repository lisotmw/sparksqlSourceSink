package com.sparkss.base.inst;

import com.sparkss.base.interf.Checkable;
import com.sparkss.base.interf.DSReader;
import com.sparkss.base.interf.mgr.DSReaderMgr;
import com.sparkss.base.interf.DSWriter;
import com.sparkss.base.interf.mgr.DSWriterMgr;
import com.sparkss.base.keys.CommonOptionKey;
import com.sparkss.base.keys.SparkOptionEnum;
import com.sparkss.base.log.Logger0;
import com.sparkss.base.pool.PoolMgr;
import com.sparkss.base.pool.ReaderObjPool;
import com.sparkss.base.pool.WriterObjPool;
import com.sparkss.base.pool.hbase.HBaseReaderPool;
import com.sparkss.base.pool.hbase.HBaseWriterPool;
import org.apache.spark.sql.sources.v2.DataSourceOptions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 不同数据库需要维护的实例，包括：
 * 需要的各层级接口实现；
 * 数据库标识 dataBaseMark
 * TODO: 实现通用的参数检测接口
 * @Author $ zho.li
 * @Date 2020/12/21 17:08
 **/
public enum DataSourceInstance implements DSWriter, DSReader, Checkable, Logger0 {
    HBASE_READ(DataBaseMark.HBASE_READ, HBaseReaderPool.class, null){
        @Override
        public boolean check(DataSourceOptions options) {
            List<SparkOptionEnum> checks = Arrays.asList(
                    SparkOptionEnum.TABLE_NAME,
                    SparkOptionEnum.SCHEMA_STR,
                    SparkOptionEnum.FAMILY_COLUMN
            );
            return checkOptions(options, checks, getLogger());
        }
    },
    HBASE_WRITE(DataBaseMark.HBASE_WRITE,null, HBaseWriterPool.class) {
        @Override
        public boolean check(DataSourceOptions options) {
            List<SparkOptionEnum> checks = Arrays.asList(
                    SparkOptionEnum.TABLE_NAME,
                    SparkOptionEnum.FAMILY_COLUMN,
                    SparkOptionEnum.HBASE_ROW_KEY
            );
            return checkOptions(options, checks, getLogger());
        }
    };

    private DataBaseMark dataBaseMark;

    private Class<? extends ReaderObjPool> readerPoolClz;
    private Class<? extends WriterObjPool> writerPoolClz;
        DataSourceInstance(DataBaseMark dataBaseMark,
                       Class<? extends ReaderObjPool> readerPoolClz,
                       Class<? extends WriterObjPool> writerPoolClz){
        this.dataBaseMark = dataBaseMark;
        this.readerPoolClz = readerPoolClz;
        this.writerPoolClz = writerPoolClz;
    }

    public String getDataBaseMark(){
        return dataBaseMark.getMark();
    }

    private static final Map<String,DataSourceInstance> map
            = Stream.of(values()).collect(Collectors.toMap(e->e.getDataBaseMark(),e->e));

    public static DataSourceInstance getInstance(String dataBaseMark){
        return map.get(dataBaseMark);
    }

    @Override
    public DSWriterMgr getDSWriterMgr(long flowId) {
        DSWriterMgr writerMgr = null;
        if (writerPoolClz != null){
            WriterObjPool writerPool = PoolMgr.POOL.getWriterPool(writerPoolClz);
            if (writerPool != null){
                writerMgr = (DSWriterMgr) writerPool.getReuse();
                writerMgr.setFlowId(flowId);
            }
        }
        return writerMgr;
    }

    @Override
    public DSReaderMgr getDSReaderMgr(long flowId) {
        DSReaderMgr readerMgr = null;
        if (readerPoolClz != null){
            ReaderObjPool readerPool = PoolMgr.POOL.getReaderPool(readerPoolClz);
            if (readerPool != null){
                readerMgr = (DSReaderMgr) readerPool.getReuse();
                readerMgr.setFlowId(flowId);
            }
        }
        return readerMgr;
    }


    public Class<? extends ReaderObjPool> getReaderPoolClz(){
        return readerPoolClz;
    }

    public Class<? extends WriterObjPool> getWriterPoolClz(){
        return writerPoolClz;
    }
}
