package com.sparkss.base;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;

public class BaseOperation {

    private BaseConnect baseConnect;
    /**
     * LiBaiheng 2020/12/22 9:55
     * @description 组装sql
     */
    public BaseConnect createBaseConnect(@Null String tableName,
                                            @NotNull String operationType,
                                            @NotNull String dbType,
                                            @NotNull Object object
                                            ){
        return baseConnect;
    }
}
