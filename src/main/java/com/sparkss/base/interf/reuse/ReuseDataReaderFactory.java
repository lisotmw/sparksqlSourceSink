package com.sparkss.base.interf.reuse;

import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 13:24
 **/
public interface ReuseDataReaderFactory<T> extends DataReaderFactory<T>,Reusable {
}
