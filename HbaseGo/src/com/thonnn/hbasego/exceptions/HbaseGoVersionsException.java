package com.thonnn.hbasego.exceptions;

/**
 * 类名字面意思
 * @author Thonnn 2018-04-02
 * @version 1.1.0
 * @since 1.1.0
 */
public class HbaseGoVersionsException extends Exception {
    public HbaseGoVersionsException(){
        super("HbaseGo 'maxVersions' mast be equal or more than -1.");
    }

    public HbaseGoVersionsException(String msg){
        super(msg);
    }
}
