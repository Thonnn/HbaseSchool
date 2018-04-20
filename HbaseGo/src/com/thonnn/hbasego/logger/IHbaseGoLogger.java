package com.thonnn.hbasego.logger;

/**
 * 日志记录器的超接口
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public interface IHbaseGoLogger {
    /**
     * 记录信息
     * @param currentObject 申请记录信息的对象
     * @param type 信息类型
     * @param msg 信息内容
     * @since 1.2.0
     */
    void recordMsg(Object currentObject, HbaseGoLogType type, String msg);
}
