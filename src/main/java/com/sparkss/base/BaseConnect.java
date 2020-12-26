package com.liz.base;

import javax.validation.constraints.NotNull;

/**
 * LiBaiheng 2020/12/21 17:09
 * @description 基类  所有数据源基类
 */
public class BaseConnect<T> {

    // sql语句
    private String sql;

    // 实体类
    private T dbEntity;

    // 查询类型
    @NotNull(message = "查询类型必须是OperationType枚举类",groups = {OperationType.class})
    private String operationType;

    // 数据库类型
    @NotNull(message = "数据类型类型必须是DBType枚举类",groups = {DBType.class})
    private String DBType;



}
