package com.sparkss.base.interf.mgr;

import com.sparkss.base.interf.reuse.Reusable;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

/**
 * 池化资源持有的可复用 Writer 资源共有父类
 * @Author $ zho.li
 * @Date 2020/12/25 6:52
 **/
public interface DSWriterMgr<T> extends Reusable {

    DataSourceWriter getDataSourceWriter(long flowId);

    DataWriterFactory<T> getDataWriterFactory(long flowId);

    DataWriter<T> getDataWriter(long flowId);}
