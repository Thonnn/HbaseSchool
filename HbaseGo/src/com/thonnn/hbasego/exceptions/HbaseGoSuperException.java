package com.thonnn.hbasego.exceptions;

import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;

/**
 * 本工程异常类的超类，增加了对日志记录器的支持
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public abstract class HbaseGoSuperException extends Exception{
    /**
     * 默认构造，增加了对日志记录器的支持
     * @param msg 异常信息
     */
    public HbaseGoSuperException(String msg){
        super(msg);
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.ERROR, this.getClass().getName() +": " + this.getMessage());
    }
}
