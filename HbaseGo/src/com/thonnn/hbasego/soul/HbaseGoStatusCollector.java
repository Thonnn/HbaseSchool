package com.thonnn.hbasego.soul;

import java.util.HashMap;

/**
 * HbaseGo 资源状态收集器
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public class HbaseGoStatusCollector {
    private static HbaseGoSoulState hbaseGoState = HbaseGoSoulState.UNKNOWN;                // HbaseGo 的状态
    private static HbaseGoSoulState selfManagingThreadState = HbaseGoSoulState.UNKNOWN;     // 自管理线程的状态
    private static int spareConnectionsNum = -1;                                            // 空闲连接数
    private static int busyConnectionsNum = -1;                                             // 繁忙连接数
    private static int busyConnectionsTimesListSize = -1;                                   // 繁忙连接数对应的自管理停滞时间列表的大小，这个值在正常情况下应该与繁忙连接数相同
    private static HbaseGoSoulState loggerState = HbaseGoSoulState.UNKNOWN;                 // 日志记录器状态
    private static final HashMap<String, String> otherStates = new HashMap<>();            // 其他待定义的状态集

    /**
     * 获取 HbaseGo 自身的状态
     * @return HbaseGo 自身的状态
     * @since 1.2.0
     */
    public synchronized static HbaseGoSoulState getHbaseGoState() {
        return hbaseGoState;
    }

    /**
     * 设置 HbaseGo 自身的状态
     * @param hbaseGoState HbaseGo 自身的状态
     * @since 1.2.0
     */
    public synchronized static void setHbaseGoState(HbaseGoSoulState hbaseGoState) {
        HbaseGoStatusCollector.hbaseGoState = hbaseGoState;
    }

    /**
     * 获取自管理线程的状态<br>
     * 自管理线程是 HbaseGo.java 中一个用于管理自身连接池的线程
     * @return 自管理线程的状态
     * @since 1.2.0
     */
    public synchronized static HbaseGoSoulState getSelfManagingThreadState() {
        return selfManagingThreadState;
    }

    /**
     * 设置自管理线程的状态<br>
     * 自管理线程是 HbaseGo.java 中一个用于管理自身连接池的线程
     * @param selfManagingThreadState 自管理线程的状态
     * @since 1.2.0
     */
    public synchronized static void setSelfManagingThreadState(HbaseGoSoulState selfManagingThreadState) {
        HbaseGoStatusCollector.selfManagingThreadState = selfManagingThreadState;
    }

    /**
     * 获取空闲连接数
     * @return 空闲连接数
     * @since 1.2.0
     */
    public synchronized static int getSpareConnectionsNum() {
        return spareConnectionsNum;
    }

    /**
     * 设置空闲连接数
     * @param spareConnectionsNum 空闲连接数
     * @since 1.2.0
     */
    public synchronized static void setSpareConnectionsNum(int spareConnectionsNum) {
        HbaseGoStatusCollector.spareConnectionsNum = spareConnectionsNum;
    }

    /**
     * 获取繁忙连接数
     * @return 繁忙连接数
     * @since 1.2.0
     */
    public synchronized static int getBusyConnectionsNum() {
        return busyConnectionsNum;
    }

    /**
     * 设置繁忙连接数
     * @param busyConnectionsNum 繁忙连接数
     * @since 1.2.0
     */
    public synchronized static void setBusyConnectionsNum(int busyConnectionsNum) {
        HbaseGoStatusCollector.busyConnectionsNum = busyConnectionsNum;
    }

    /**
     * 获取繁忙连接对应的停滞时间列表的大小
     * @return 繁忙连接对应的停滞时间列表的大小
     * @since 1.2.0
     */
    public static int getBusyConnectionsTimesListSize() {
        return busyConnectionsTimesListSize;
    }

    /**
     * 设置繁忙连接对应的停滞时间列表的大小
     * @param busyConnectionsTimesListSize 繁忙连接对应的停滞时间列表的大小
     * @since 1.2.0
     */
    public static void setBusyConnectionsTimesListSize(int busyConnectionsTimesListSize) {
        HbaseGoStatusCollector.busyConnectionsTimesListSize = busyConnectionsTimesListSize;
    }

    /**
     * 获取日志记录器的状态
     * @return 日志记录器的状态
     * @since 1.2.0
     */
    public synchronized static HbaseGoSoulState getLoggerState() {
        return loggerState;
    }

    /**
     * 设置日志记录器的状态
     * @param loggerState 日志记录器的状态
     * @since 1.2.0
     */
    public synchronized static void setLoggerState(HbaseGoSoulState loggerState) {
        HbaseGoStatusCollector.loggerState = loggerState;
    }

    /**
     * 获取其他状态信息
     * @param key 要获取的信息对应的键
     * @return 状态信息
     * @since 1.2.0
     */
    public synchronized static String getOtherState(String key){
        return otherStates.get(key);
    }

    /**
     * 设置一条状态信息
     * @param key 键
     * @param value 值
     * @since 1.2.0
     */
    public synchronized static void setOtherState(String key, String value){
        otherStates.put(key, value);
    }
}
