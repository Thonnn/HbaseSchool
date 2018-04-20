package com.thonnn.hbasego.logger;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 默认日志打印机
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public final class HbaseGoDefaultLogPrinter implements IHbaseGoLogPrinter {
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

    @Override
    public synchronized void print(Object currentObject, HbaseGoLogType type, String msg) {
        String msgStr = getTime() + "_" + type.toString() + ": " + msg;
        System.out.println(msgStr);
    }

    /**
     * 获取时间字符串
     * @return 时间字符串
     * @since 1.2.0
     */
    private synchronized String getTime(){
        return "[" + dateFormat.format(new Date()) + "]";
    }
}
