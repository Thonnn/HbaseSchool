package com.thonnn.hbasego.logger;

/**
 * 日志类型
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public enum HbaseGoLogType {
    /**
     * 普通信息类型
     * @since 1.2.0
     */
    INFO,
    /**
     * 警告信息类型
     * @since 1.2.0
     */
    WARN,
    /**
     * 错误信息类型
     * @since 1.2.0
     */
    ERROR;

    /**
     * 获取信息类型对应的字符串
     * @return 信息类型对应的字符串
     * @since 1.2.0
     */
    @Override
    public String toString() {
        switch (this){
            case INFO:
                return "[ INFO]";
            case WARN:
                return "[ WARN]";
            case ERROR:
                return "[ERROR]";
            default:
                return "[ NULL]";
        }
    }
}
