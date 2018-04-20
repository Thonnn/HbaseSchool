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
public interface IHbaseGoDAOAlter {
    /**
     * 单个修改数据，请注意在初始化本类时的 alterExistCheck 参数
     * @param bean 欲操作的对象
     * @return  返回是否修改成功
     * @since 1.0.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean alter(IHbaseGoBean bean){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.alter(IHbaseGoBean) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(bean.getClass().getName()).tableNmae);
        return false;
    }

    /**
     * 批量修改数据，请注意在初始化本类时的 alterExistCheck 参数；
     * 支持实现了 IHBaseGoBean 的多个不同类 bean 对象同时修改；
     * 即：可以使用一个 List 同时存储实现于接口 IHBaseGoBean 的多个不同类 Temp1、Temp2、Temp3 …… 的多个不同对象同时数据修改。
     * @param beanList 欲修改的 bean 的列表，
     * @return 是否修改成功
     * @since 1.0.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean alter(List<IHbaseGoBean> beanList){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.alter(List<IHbaseGoBean>) to Hbase. List Size = " + beanList.size());
        return false;
    }
}
