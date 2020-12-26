package com.sparkss.base.interf.reuse;

import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 13:27
 **/
public interface ReuseDataSourceWriter extends DataSourceWriter,Reusable {
}
