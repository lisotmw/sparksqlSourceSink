package com.sparkss.base.interf.reuse;

import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 13:28
 **/
public interface ReuseDataWriterFactory<T> extends DataWriterFactory<T>,Reusable {
}
