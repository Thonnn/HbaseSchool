package com.thonnn.hbasego.assists;

import com.thonnn.hbasego.exceptions.HbaseGoVersionsException;
import com.thonnn.hbasego.interfaces.IHbaseGoBean;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 一个用于存储Hbase不同版本数据的Bean，其由时间戳和继承了IHbaseGoBean的Bean实例构成。
 * @param <T> 泛型参数，其上界为IHbaseGoBean
 * @author Thonnn 2018-04-02
 * @version 1.1.0
 * @since 1.1.0
 */
public class HbaseGoVersionBean<T extends IHbaseGoBean>{
    private List<Time_Bean> list = new ArrayList<>();

    /**
     * 添加一个版本的 IHbaseGoBean
     * @param timestamp 时间戳
     * @param bean 实现了接口 IHbaseGoBean 的实例
     * @since 1.1.0
     */
    public void add(long timestamp, T bean){
        //先添加，后重新排序
        list.add(new Time_Bean(timestamp, bean));
        list.sort(Comparator.comparingLong(tb -> tb.timestamp));            // 从老版本到新版本的排序（堆叠）方案，确使0位置为最老版本
    }

    /**
     * 获取所有版本 Bean 实例构成的列表
     * @return 一个存储了所有版本的 Bean 实例的列表，其默认排序为：0 位置为最老版本
     * @since 1.1.0
     */
    public List<T> getList() {
        List<T> rsl = new ArrayList<>();                // 需要重新封装 结果 List
        for (Time_Bean bean : list) {
            rsl.add(bean.bean);
        }
        return rsl;
    }

    /**
     * 获取该记录在 Hbase 中存有的版本数量
     * @return 该记录在 Hbase 中存有的版本数量
     * @since 1.1.0
     */
    public int getVersionsNum(){
        return list.size();                 // list的大小即是版本的数量
    }

    /**
     * 获取第一个版本，即最老版本的数据。
     * @return 最老版本的数据
     * @since 1.1.0
     */
    public T getFirstVersionBean(){
        return list.get(0).bean;                    // 根据已排序的结果，0位置即为最老的版本
    }

    /**
     * 获取最后一个版本，即最新版本的数据
     * @return 最新版本的数据
     * @since 1.1.0
     */
    public T getLastVersionBean(){
        return list.get(list.size() - 1).bean;                      // 根据已排序的结果，最后一个即为最新的版本
    }

    /**
     * 通过版本序号获取实例，注意： 为协同Hbase自身，规定 1 为最老版本
     * @param version 版本号
     * @return 对应的数据 Bean 实例
     * @since 1.1.0
     */
    public T getBeanByVersion(int version){
        if(version <= 0){
            try {
                throw new HbaseGoVersionsException("HbaseGo: Data VERSION mast be more than 0.");
            } catch (HbaseGoVersionsException e) {
                e.printStackTrace();
            }
        }
        return list.get(version - 1).bean;                      // 因为版本号从1开始，list的索引从0开始，所以要减去1
    }

    /**
     * 通过时间戳获取对应版本的数据 Bean
     * @param timestamp 时间戳
     * @return 对应的数据 Bean，当该数据不存在时返回 null
     * @since 1.1.0
     */
    public T getBeanByTimestamp(long timestamp){
        for (Time_Bean bean : list) {
            if(bean.timestamp == timestamp){
                return bean.bean;
            }
        }
        return null;
    }

    /**
     * 根据 Bean 反查其对应的时间戳
     * @param bean 需要查询的 Bean
     * @return 其对应的时间戳
     * @since 1.1.0
     */
    public long getTimestampByBean(T bean){
        for (Time_Bean b : list){
            if(bean == b.bean){
                return b.timestamp;
            }
        }
        return 0;
    }

    /**
     * 一个存储时间戳于Bean对应关系的内部类
     * @version 1.1.0
     * @since 1.1.0
     */
    private class Time_Bean {
        long timestamp;
        T bean;

        /**
         * 主体构造
         * @param timestamp 时间戳
         * @param bean 实现了接口 IHbaseGoBean 的实例，其类型必须与主类泛型参数类型相同
         */
        Time_Bean(long timestamp, T bean){
            this.timestamp = timestamp;
            this.bean = bean;
        }
    }
}
