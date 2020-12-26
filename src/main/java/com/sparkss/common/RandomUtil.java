package com.sparkss.common;

/**
 * @Author $ zho.li
 * @Date 2020/12/26 9:09
 **/
public class RandomUtil {

    /**
     * 生成 n 位 long
     * @param n
     * @return
     */
    public static long generateRandomNumber(int n){
        if(n<1){
            throw new IllegalArgumentException("随机数位数必须大于0");
        }
        return (long)(Math.random()*9*Math.pow(10,n-1)) + (long)Math.pow(10,n-1);
    }
}
