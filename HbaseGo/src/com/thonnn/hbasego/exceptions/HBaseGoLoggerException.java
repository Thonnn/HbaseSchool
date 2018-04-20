package com.thonnn.hbasego.exceptions;

import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;

/**
 * 类名字面意思
 * @author Thonnn 2017-11-26
 * @version 1.0.0
 * @since 1.0.0
 */
public class HBaseGoLoggerException extends HbaseGoSuperException {
    public HBaseGoLoggerException(String msg){
        super(msg);
    }
}
