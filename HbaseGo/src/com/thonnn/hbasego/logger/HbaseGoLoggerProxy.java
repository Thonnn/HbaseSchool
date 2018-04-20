package com.thonnn.hbasego.logger;

import com.thonnn.hbasego.soul.HbaseGoSoulState;
import com.thonnn.hbasego.soul.HbaseGoStatusCollector;

/**
 * 日志记录器的代理者
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public final class HbaseGoLoggerProxy{
    private static IHbaseGoLogger logger = null;

    /**
     * 获取设定的日志记录器
     * @return 日志记录器
     * @since 1.2.0
     */
    public static IHbaseGoLogger getLogger() {
        return logger;
    }

    /**
     * 设定日志记录器，其类型是实现了 IHbaseGoLogger 的类的对象
     * @param logger 日志记录器
     * @since 1.2.0
     */
    public static void setLogger(IHbaseGoLogger logger) {
        if (logger != null){
            HbaseGoStatusCollector.setLoggerState(HbaseGoSoulState.ENABLE);
            recordMsg(logger, HbaseGoLogType.WARN, "Logger is enabled.");
        }else {
            HbaseGoStatusCollector.setLoggerState(HbaseGoSoulState.DISABLE);
            recordMsg(null, HbaseGoLogType.WARN, "Logger is disabled.");
        }
        HbaseGoLoggerProxy.logger = logger;
    }

    /**
     * 记录信息
     * @param currentObject 申请记录信息的对象
     * @param type 信息类型
     * @param msg 信息内容
     * @since 1.2.0
     */
    public synchronized static void recordMsg(Object currentObject, HbaseGoLogType type, String msg) {
        if (logger != null) {
            logger.recordMsg(currentObject, type, msg);
        }
    }
}
