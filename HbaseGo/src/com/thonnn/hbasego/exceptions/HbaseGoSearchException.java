package com.thonnn.hbasego.exceptions;

/**
 * 类名字面意思
 * @author Thonnn 2017-11-26
 */
public class HbaseGoSearchException extends Exception{
    public HbaseGoSearchException(){
        super("HbaseGo search data error.");
    }
    public HbaseGoSearchException(String msg){
        super(msg);
    }
}
