package com.sparkss.base.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author $ zho.li
 * @Date 2021/3/16 12:43
 **/
public interface Logger0 {

    default Logger getLogger(){
        final Class<? extends Logger0> aClass = this.getClass();
        System.out.println(aClass);
        return LoggerFactory.getLogger(aClass);
    }
}
