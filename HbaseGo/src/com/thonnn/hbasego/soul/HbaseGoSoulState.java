package com.thonnn.hbasego.soul;

/**
 * HbaseGo 各种资源状态的枚举类型
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public enum HbaseGoSoulState {
    /**
     * 未知的状态
     * @since 1.2.0
     */
    UNKNOWN,
    /**
     * 已经创建的状态
     * @since 1.2.0
     */
    BUILT,
    /**
     * 未创建状态
     * @since 1.2.0
     */
    NOT_BUILD,
    /**
     * 已启用状态
     * @since 1.2.0
     */
    ENABLE,
    /**
     * 未启用状态
     * @since 1.2.0
     */
    DISABLE,
    /**
     * 正在运行状态
     * @since 1.2.0
     */
    RUNNING,
    /**
     * 停止运行状态
     * @since 1.2.0
     */
    STOPPED,
    /**
     * 错误状态
     * @since 1.2.0
     */
    ERROR;

    /**
     * 获取状态对应的字符串
     * @return 状态对应的字符串
     * @since 1.2.0
     */
    @Override
    public String toString() {
        switch (this){
            case UNKNOWN:
                return "UNKNOWN";
            case BUILT:
                return "BUILT";
            case NOT_BUILD:
                return "NOT_BUILD";
            case ENABLE:
                return "ENABLE";
            case DISABLE:
                return "DISABLE";
            case RUNNING:
                return "RUNNING";
            case STOPPED:
                return "STOPPED";
            case ERROR:
                return "ERROR";
            default:
                return "NULL";
        }
    }
}
