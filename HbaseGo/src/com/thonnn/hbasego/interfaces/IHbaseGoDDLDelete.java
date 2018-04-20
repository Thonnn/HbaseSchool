package com.thonnn.hbasego.interfaces;

import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;

/**
 * 接口名字面意思
 * @author Thonnn 2018-04-19
 * @version 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
 * @since 1.2.0 初次定义
 * @see com.thonnn.hbasego.ddl.HbaseGoDDL
 */
public interface IHbaseGoDDLDelete {

    /**
     * 删除表
     * @param tableName 要删除的表名称
     * @return 是否删除成功
     * @since 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean deleteTable(String tableName){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDDL.deleteTable(String) to HbaseTable: " + tableName);
        return false;
    }
}
