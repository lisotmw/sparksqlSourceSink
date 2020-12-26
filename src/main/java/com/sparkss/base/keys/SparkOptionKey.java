package com.sparkss.base.keys;

/**
 * @Author $ zho.li
 * @Date 2020/12/21 16:45
 **/
public interface SparkOptionKey {
    /** 表名 */
    public String TABLE_NAME = "table_name";

    /** schema信息（`name` STRING,`score` STRING） */
    public String SCHEMA_STR = "schema";

    /** hbase 列族列名（cf:name,cf:score） */
    public String FAMILY_COLUMN = "family_column";

    /** 数据库名（当前支持hbase，redis，以及读写模式，参考 @link{DataBaseMark}） */
    public String DATA_BASE_MODE = "data_base_mode";

    public String SOURCE_CLASS = "com.liz.base.BaseDataSourceV";
}
