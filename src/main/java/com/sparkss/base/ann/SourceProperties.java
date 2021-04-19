package com.sparkss.base.ann;


import java.lang.annotation.*;

/**
 * 资源配置注解，包含每一次查询的基础配置信息
 * @Author $ zho.li
 * @Date 2020/12/18 9:54
 **/
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Deprecated
public @interface SourceProperties{
    /** 表名 */
    String tableName();
    /** schema */
    String schema();
    /**映射表之后的列信息（cf:name,cf:score） */
    String sparkSchema();
}
