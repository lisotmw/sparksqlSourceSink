package com.sparkss.base.pool;

/**
 * @Author $ zho.li
 * @Date 2020/12/25 15:19
 **/

import com.sparkss.base.interf.reuse.Reusable;
import org.spark_project.jetty.util.log.Log;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.Semaphore;

/**
 * 泛型实例不能直接初始化
 * 怎么实现动态扩容啊。。。。
 * @param <T>
 */
public abstract  class ObjectPool<T extends Reusable> {
    final List<T> pool;
    final Semaphore semaphore;
    ObjectPool(T t){
        pool = new Vector<T>(getSize());
        for(int i = 0; i <getSize();i++){
            pool.add(t);
        }
        semaphore = new Semaphore(getSize());
    }

    public T getReuse(){
        T t = null;
        try{
            semaphore.acquire();
            t = pool.remove(0);
        }catch (InterruptedException e){
            e.printStackTrace();
            semaphore.release();
        }
        Log.getLogger(this.getClass()).info("我从对象池获取了一个对象，对象类型是： " + t.getClass().getSimpleName());
        return t;
    }

    public void reBack(T t){
        if(semaphore.availablePermits() < getSize()){
            pool.add(t);
            semaphore.release();
            Log.getLogger(this.getClass()).info("我向对象池归还了一个对象，对象类型是： " + t.getClass().getSimpleName());
        }
    }

    protected abstract int getSize();
}
