package com.thonnn.hbasego.exceptions;

/**
 * 类名字面意思
 * @author Thonnn 2017-11-26
 * @version 1.0.0
 * @since 1.0.0
 */
public class HbaseGoBuilderException extends Exception {
    public HbaseGoBuilderException(){
        super("HbaseGo build error.");
    }
    public HbaseGoBuilderException(String msg){
        super(msg);
    }
}
