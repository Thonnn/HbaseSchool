package com.thonnn.hbasego.exceptions;

/**
 * 类名字面意思
 * @author Thonnn 2017-11-26
 */
public class HbaseConnectException extends Exception {
    public HbaseConnectException(){
        super("Connect to Hbase error.");
    }
    public HbaseConnectException(String msg){
        super(msg);
    }
}
