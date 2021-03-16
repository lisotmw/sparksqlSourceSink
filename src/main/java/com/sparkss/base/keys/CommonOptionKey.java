package com.sparkss.base.keys;

/**
 * @Author $ zho.li
 * @Date 2020/12/21 16:45
 **/
public interface CommonOptionKey {
    /** 表名 */
    public String TABLE_NAME = "table_name";

    /**
     * hbase rowkey
     */
    public String HBASE_ROW_KEY = "hbase_row_key";

    /**
     * 不带属性的字段名
     */
    public String FIELDS = "fields";

    /**
     *  hbase region 个数
     */
    public String HBASE_REGIONS = "hbase_regions";

    /** schema信息（`name` STRING,`score` STRING） */
    public String SCHEMA_STR = "schema";

    /** hbase 列族列名（cf:name,cf:score） */
    public String FAMILY_COLUMN = "family_column";

    /** 数据库名（当前支持hbase，redis，以及读写模式，参考 @link{DataBaseMark}） */
    public String DATA_BASE_MODE = "data_base_mode";

    public String SOURCE_CLASS = "com.liz.base.BaseDataSourceV";
}
