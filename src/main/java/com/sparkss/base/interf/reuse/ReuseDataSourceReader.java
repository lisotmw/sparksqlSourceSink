package com.sparkss.base.interf.reuse;

import org.apache.spark.sql.sources.v2.reader.DataSourceReader;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 13:22
 **/
public interface ReuseDataSourceReader extends DataSourceReader,Reusable {
}
