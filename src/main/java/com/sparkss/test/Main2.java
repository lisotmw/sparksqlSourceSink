package com.sparkss.test;

import com.sparkss.common.RandomUtil;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 16:41
 **/
public class Main2 {
    public static void main(String[] args) {
//        PoolMgr.POOL.init();
//        PoolMgr.POOL.getReaderPoolMap().entrySet().forEach(System.out::println);
//        System.out.println(RandomUtil.generateRandomNumber(9));

        String a = "`123d`dfwersS9";

        String a1 = a.split("\\`")[1];
        System.out.println(a1);


        // 后置断言只能匹配 指定字符？？？？
        String ll = a.replaceAll("(?<=[\\`\\w{1,}\\`])[\\w{0,}]", "");
        System.out.println(ll);
        //out: ``

        String s = a.replaceAll("(?<=[\\`\\w{1,}\\`])dfwers", "");
        System.out.println(s);// out: `123d`S9
        String s1 = s.replaceAll("(?<=[\\`\\w{1,}\\`])S", "");
        System.out.println(s1);// out: `123d`9
        String s2 = s1.replaceAll("(?<=[\\`\\w{1,}\\`])9", "");
        System.out.println(s2);// out: `123d`

    }
}
