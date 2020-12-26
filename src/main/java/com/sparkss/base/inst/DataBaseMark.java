package com.sparkss.base.inst;

/**
 * @Author $ zho.li
 * @Date 2020/12/23 12:07
 **/
public enum DataBaseMark {
    HBASE_READ("hbase_read"),
    HBASE_WRITE("hbase_write"),
    HBASE_RW("hbase_rw"),

    REDIS_READ("redis_read"),
    REDIS_WRITE("redis_write"),
    REDIS_RW("redis_rw"),
    ;
    private String dataBaseMark;
    DataBaseMark(String dataBaseMark){
        this.dataBaseMark = dataBaseMark;
    }

    public String getMark(){
        return dataBaseMark;
    }
}
