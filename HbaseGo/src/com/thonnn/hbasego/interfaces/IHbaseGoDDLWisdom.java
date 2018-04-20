package com.thonnn.hbasego.interfaces;

import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;

/**
 * 接口名字面意思，表示一些无法具体命名的操作
 * @author Thonnn 2018-04-19
 * @version 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
 * @since 1.2.0 初次定义
 * @see com.thonnn.hbasego.ddl.HbaseGoDDL
 */
public interface IHbaseGoDDLWisdom {

    /**
     * 清空表中数据
     * @param tableName 要清空的表的名称
     * @param preserveSplits Hbase 清空表操作中需要指定的一个字段：“保留拆分”，默认的 true；
     * @return 是否清空成功
     * @since 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean truncateTable(String tableName, boolean preserveSplits){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDDL.truncateTable(String, boolean) to HbaseTable: " + tableName +" preserveSplits = " + preserveSplits);
        return false;
    }

    /**
     * 清空表中数据，使 preserveSplits = true 进行的默认操作
     * @param tableName 要清空的表的名称
     * @return 是否清空成功
     * @since 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean truncateTable(String tableName){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDDL.truncateTable(String) to HbaseTable: " + tableName);
        return false;
    }

    /**
     * 检测表是否存在
     * @param tableName 要检测的表名称
     * @return 是否存在
     * @since 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean existTable(String tableName){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDDL.existTable(String) to HbaseTable: " + tableName);
        return false;
    }
}
