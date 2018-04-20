package com.thonnn.hbasego.logger;

/**
 * 日至打印机的超接口
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public interface IHbaseGoLogPrinter {
    /**
     * 打印信息
     * @param currentObject 申请记录日志信息的对象
     * @param type 日志类型
     * @param msg 日志内容
     * @since 1.2.0
     */
    void print(Object currentObject, HbaseGoLogType type, String msg);
}
