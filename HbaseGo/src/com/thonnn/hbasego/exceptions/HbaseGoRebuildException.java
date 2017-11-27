package com.thonnn.hbasego.exceptions;

/**
 * 类名字面意思
 * @author Thonnn 2017-11-26
 */
public class HbaseGoRebuildException extends Exception {
    public HbaseGoRebuildException(){
        super("HbaseGo was built and you cannot re-build it again.");
    }

    public HbaseGoRebuildException(String msg){
        super(msg);
    }
}
