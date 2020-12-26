package com.sparkss.base.interf;

import org.apache.spark.sql.sources.v2.DataSourceOptions;

/**
 * 参数校验接口
 * @Author $ zho.li
 * @Date 2020/12/26 8:45
 **/
public interface Checkable {
    boolean check(DataSourceOptions options);
}
