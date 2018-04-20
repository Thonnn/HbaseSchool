package com.thonnn.hbasego.interfaces;

import com.thonnn.hbasego.ddl.HbaseGoTable;
import com.thonnn.hbasego.ddl.HbaseGoTableFamily;
import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;

/**
 * 接口名字面意思
 * @author Thonnn 2018-04-19
 * @version 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
 * @since 1.2.0 初次定义
 * @see com.thonnn.hbasego.ddl.HbaseGoDDL
 */
public interface IHbaseGoDDLAlter {
    /**
     * 添加一个列簇
     * @param tableName 表名称，将列簇添加到的表
     * @param family HbaseGoFamily 列簇对象
     * @return 是否添加成功
     * @since 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean addFamily(String tableName, HbaseGoTableFamily family){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDDL.addFamily(String, HbaseGoTableFamily) to HbaseTable: " + tableName);
        return false;
    }

    /**
     * 删除列簇
     * @param tableName 表名称，要删除的列簇所在的表
     * @param familyName 列簇名称
     * @return 是否删除成功
     * @since 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean deleteFamily(String tableName, String familyName){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDDL.deleteFamily(String, String) to HbaseTable: " + tableName +" Family: " + familyName);
        return false;
    }

    /**
     * 修改列簇
     * @param tableName 表名称，要修改的列簇所在的表
     * @param family HbaseGoFamily 列簇对象
     * @return 是否修改成功
     * @since 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean alterFamily(String tableName, HbaseGoTableFamily family){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDDL.alterFamily(String, HbaseGoTableFamily) to HbaseTable: " + tableName +" Family: " + family.NAME);
        return false;
    }

    /**
     * HbaseGoTable 对象的结构修改整张表
     * @param hbaseGoTable 要修改的表，其 tableName 属性必须指定要修改的表名称
     * @return 是否修改成功
     * @since 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DDL 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default boolean alterTable(HbaseGoTable hbaseGoTable){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDDL.alterTable(HbaseGoTable) to HbaseTable: " + hbaseGoTable.getTableName());
        return false;
    }
}
