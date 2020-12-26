package com.sparkss.base.pool;

import com.sparkss.base.interf.mgr.DSReaderMgr;
import com.sparkss.base.interf.mgr.DSWriterMgr;
import org.reflections.Reflections;
import org.reflections.scanners.FieldAnnotationsScanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * 对象池管理单例
 * @Author $ zho.li
 * @Date 2020/12/25 16:27
 **/
public enum PoolMgr {
    POOL,;
    private Map<Class<? extends ReaderObjPool>,ReaderObjPool>
            readerPool = new ConcurrentHashMap<>();
    private Map<Class<? extends WriterObjPool>,WriterObjPool>
            writerPool = new ConcurrentHashMap<>();
    public Map<Class<? extends ReaderObjPool>,ReaderObjPool> getReaderPoolMap(){
        return readerPool;
    }

    public Map<Class<? extends WriterObjPool>,WriterObjPool> getWriterPoolMap(){
        return writerPool;
    }

    public ReaderObjPool getReaderPool(Class<? extends ReaderObjPool> clazz){
        ReaderObjPool pool = null;
        if (readerPool.containsKey(clazz))
            pool = readerPool.get(clazz);
        return pool;
    }

    public WriterObjPool getWriterPool(Class<? extends WriterObjPool> clazz){
        WriterObjPool pool = null;
        if (writerPool.containsKey(clazz))
            pool = writerPool.get(clazz);
        return pool;
    }

    public void init(){
        String pkg = getClass().getPackage().getName();
        System.out.println(pkg);
        // 初始化工具类
        Reflections reflections = new Reflections(
                new ConfigurationBuilder().
                        forPackages(pkg).
                        addScanners(new SubTypesScanner()).
                        addScanners(new FieldAnnotationsScanner()));

        // WriterMgr 和 ReaderMgr 实现类包路径
        String rwPkg = pkg.replace("pool","impl");
        Map<String,Class<?>> typeCaches = new HashMap<>();
        // 反射取到各个 WriterMgr 和 WriterMgr 的实现类，放进缓存
        Set<Class<? extends DSReaderMgr>> readerMgrs = reflections.getSubTypesOf(DSReaderMgr.class);
        readerMgrs.forEach(one->{typeCaches.put(one.getTypeName(),one);});
        Set<Class<? extends DSWriterMgr>> writerMgrs = reflections.getSubTypesOf(DSWriterMgr.class);
        writerMgrs.forEach(one->{typeCaches.put(one.getTypeName(),one);});

        // 获取 reader 对象池子类
        Set<Class<? extends ReaderObjPool>> readerPoolClzs =
                reflections.getSubTypesOf(ReaderObjPool.class);
        for(Class<? extends ReaderObjPool> readerPoolClz : readerPoolClzs){
            // 取到泛型的类型参数
            Type t = readerPoolClz.getGenericSuperclass();
            Type type = ((ParameterizedType) t).getActualTypeArguments()[0];
            String totleTypeName = type.getTypeName();

            Class<?> readerMgrClazz = typeCaches.get(totleTypeName);
            try{
                // 实例化对象池需要的对象
                Object o = readerMgrClazz.newInstance();
                Constructor<? extends ReaderObjPool> constructor =
                        readerPoolClz.getDeclaredConstructor(readerMgrClazz);
                ReaderObjPool readerObjPool = constructor.newInstance(o);
                readerPool.put(readerPoolClz,readerObjPool);
            }catch (InstantiationException e){
                e.printStackTrace();
            }catch (IllegalAccessException e){
                e.printStackTrace();
            }catch (NoSuchMethodException e){
                e.printStackTrace();
            }catch (IllegalArgumentException e){
                e.printStackTrace();
            }catch (InvocationTargetException e){
                e.printStackTrace();
            }
        }

        // 获取 writer 对象池子类
        Set<Class<? extends WriterObjPool>> writerPoolClzs =
                reflections.getSubTypesOf(WriterObjPool.class);
        for(Class<? extends WriterObjPool> writerPoolClz : writerPoolClzs){
            // 取到泛型的类型参数
            Type t = writerPoolClz.getGenericSuperclass();
            Type type = ((ParameterizedType) t).getActualTypeArguments()[0];
            String totleTypeName = type.getTypeName();

            Class<?> writerMgrClazz = typeCaches.get(totleTypeName);
            try{
                // 实例化对象池需要的对象
                Object o = writerMgrClazz.newInstance();
                Constructor<? extends WriterObjPool> constructor =
                        writerPoolClz.getDeclaredConstructor(writerMgrClazz);
                WriterObjPool writerObjPool = constructor.newInstance(o);
                writerPool.put(writerPoolClz,writerObjPool);
            }catch (InstantiationException e){
                e.printStackTrace();
            }catch (IllegalAccessException e){
                e.printStackTrace();
            }catch (NoSuchMethodException e){
                e.printStackTrace();
            }catch (IllegalArgumentException e){
                e.printStackTrace();
            }catch (InvocationTargetException e){
                e.printStackTrace();
            }
        }

    }
}
