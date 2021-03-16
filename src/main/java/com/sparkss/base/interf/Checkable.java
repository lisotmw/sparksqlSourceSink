package com.sparkss.base.interf;

import com.sparkss.base.keys.SparkOptionEnum;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.slf4j.Logger;

import java.util.List;
import java.util.Optional;

/**
 * 参数校验接口
 * @Author $ zho.li
 * @Date 2020/12/26 8:45
 **/
public interface Checkable {
    boolean check(DataSourceOptions options);

    default boolean checkOptions(DataSourceOptions options, List<SparkOptionEnum> optionEnum, Logger logger){
        if (optionEnum == null)
            return true;
        for(SparkOptionEnum check : optionEnum){
            Optional<String> checkVal = options.get(check.getKey());
            // 检验是否有值
            if (!checkVal.isPresent()){
                logger.error("缺乏必要参数：" + check.getKey());
                return false;
            }
            // 对传递值的校验
            if(!check.check(checkVal.get())){
                logger.error("参数类型校验不合法：" + check.getKey());
                return false;
            }
        }
        return true;
    }
}
