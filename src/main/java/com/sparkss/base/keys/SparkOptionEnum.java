package com.sparkss.base.keys;

import com.sparkss.base.log.Logger0;

/**
 * @Author $ zho.li
 * @Date 2021/3/16 10:39
 **/
public enum SparkOptionEnum implements Logger0 {

    /** schema信息（`name` STRING,`score` STRING） */
    SCHEMA_STR(CommonOptionKey.SCHEMA_STR){
        @Override
        public boolean check(Object param) {
            try{
                String strParam = (String)param;
                if (strParam == null || strParam.length() == 0)
                    return false;
                String[] schemas = strParam.split(",");
                for(String schema : schemas){
                    // legal Type: `field_name`Type
                    if (schema.split("\\`").length < 3){
                        return false;
                    }
                }
                return true;
            }catch (Exception e){
                getLogger().error("指定的key格式不合法：" + getKey());
                return false;
            }
        }
    },
    TABLE_NAME(CommonOptionKey.TABLE_NAME),

    /**
     * hbase rowkey
     */
    HBASE_ROW_KEY (CommonOptionKey.HBASE_ROW_KEY),

    /**
     * 不带属性的字段名
     */
    FIELDS (CommonOptionKey.FIELDS ),

    HBASE_REGIONS(CommonOptionKey.HBASE_REGIONS){
        @Override
        public boolean check(Object param) {
            try {
                if(!(param instanceof Integer)){
                    return true;
                }
                int regions = (int)param;
                return true;
            }catch (ClassCastException e){
                getLogger().error("hbase region need to be Integer!, key: " + getKey());
                return false;
            }
        }
    },

    /** hbase 列族列名（cf:name,cf:score） */
    FAMILY_COLUMN (CommonOptionKey.FAMILY_COLUMN ){
        @Override
        public boolean check(Object param) {
           try{
               String cf = (String) param;
               if(cf == null || cf.length() == 0){
                   return false;
               }
               String[] cfs = cf.split(",");
               for(String cf0 : cfs){
                   if (cf0.split(":").length < 2)
                       return false;
               }
               return true;
           }catch (ClassCastException e){
               getLogger().error("指定的key格式不合法：" + getKey());
               return false;
           }
        }
    },
    ;

    private String key;

    SparkOptionEnum(String key){
        this.key = key;
    }

    public boolean check(Object param){
        return true;
    }

    public String getKey(){
        return key;
    }
}
