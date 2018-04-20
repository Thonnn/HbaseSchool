package com.thonnn.hbasego.interfaces;

import com.thonnn.hbasego.HbaseGo;
import com.thonnn.hbasego.dao.HbaseGoDAO;
import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;

import java.util.List;

/**
 * 接口名字面意思
 * @author Thonnn 2017-11-26
 * @version 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
 * @since 1.0.0 初次定义
 * @see HbaseGoDAO
 */
public interface IHbaseGoDAODelete {
    /**
     * 单条删除，需要在 bean 中的 rowkey 映射字段存储了rowkey 的值
     * @param bean 欲操作的 bean
     * @return 是否删除成功
     * @since 1.0.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean delete(IHbaseGoBean bean){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.delete(IHbaseGoBean) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(bean.getClass().getName()).tableNmae);
        return false;
    }

    /**
     * 批量删除，需要在 bean 中的 rowkey 映射字段存储了rowkey 的值；
     * 支持实现了 IHBaseGoBean 的多个不同类 bean 数据同时删除；
     * 即：可以使用一个 List 同时存储实现于接口 IHBaseGoBean 的多个不同类 Temp1、Temp2、Temp3 …… 的多个不同对象同时数据删除。
     * @param beanList 欲操作的 bean 的列表
     * @return 是否删除成功
     * @since 1.0.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean delete(List<IHbaseGoBean> beanList){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.delete(List<IHbaseGoBean>) to Hbase. List Size = " + beanList.size());
        return false;
    }
}
