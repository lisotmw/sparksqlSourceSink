package com.sparkss.base.interf.reuse;

import org.apache.spark.sql.sources.v2.writer.DataWriter;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 13:29
 **/
public interface ReuseDataWriter<T> extends DataWriter<T>,Reusable {
}
