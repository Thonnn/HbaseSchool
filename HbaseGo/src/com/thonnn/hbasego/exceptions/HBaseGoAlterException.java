package com.thonnn.hbasego.exceptions;

/**
 * 类名字面意思
 * @author Thonnn 2017-11-26
 */
public class HBaseGoAlterException extends Exception {
    public HBaseGoAlterException(){
        super("HBaseGo alter data error.");
    }
    public HBaseGoAlterException(String msg){
        super(msg);
    }
}
