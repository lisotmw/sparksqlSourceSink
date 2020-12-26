package com.sparkss.base.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author $ zho.li
 * @Date 2020/12/24 19:32
 **/
public class Log {
    public static Logger getLogger(final Class<?> clazz){
        return LoggerFactory.getLogger(clazz);
    }
}
