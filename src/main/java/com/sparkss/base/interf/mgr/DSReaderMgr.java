package com.sparkss.base.interf.mgr;

import com.sparkss.base.interf.reuse.Reusable;
import org.apache.spark.sql.sources.v2.reader.DataReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

/**
 * 池化 Reader 资源持有的公共父类
 * @Author $ zho.li
 * @Date 2020/12/25 6:50
 **/
public interface DSReaderMgr<T> extends Reusable {

    DataSourceReader getDataSourceReader(long flowId);

    DataReaderFactory<T> getDataReaderFactory(long flowId);

    DataReader<T> getDataReader(long flowId);

}
