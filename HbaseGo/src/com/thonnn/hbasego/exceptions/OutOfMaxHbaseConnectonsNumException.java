package com.thonnn.hbasego.exceptions;

/**
 * 类名字面意思
 * @author Thonnn 2017-11-26
 * @version 1.0.0
 * @since 1.0.0
 */
public class OutOfMaxHbaseConnectonsNumException extends Exception {
	public OutOfMaxHbaseConnectonsNumException(){
		super("Out of max hbase connectons num.");
	}
	public OutOfMaxHbaseConnectonsNumException(String msg){
		super(msg);
	}
}
