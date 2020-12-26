package com.sparkss.base.exception;

/**
 * @Author $ zho.li
 * @Date 2020/12/18 12:28
 **/
public class AnnMissException extends Exception{

    @Deprecated
    public AnnMissException(String message) {
    }

    public AnnMissException(Class clazz){
        super(clazz.getName() + " should be declared by annotation: SourceProperties");
    }
}
