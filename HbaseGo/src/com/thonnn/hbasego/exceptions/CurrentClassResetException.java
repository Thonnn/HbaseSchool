package com.thonnn.hbasego.exceptions;

/**
 * 类名字面意思
 * @author Thonnn 2017-11-26
 */
public class CurrentClassResetException extends Exception{
    public CurrentClassResetException(){
        super("Current-Class was set and you cannot reset it again.");
    }

    public CurrentClassResetException(String msg){
        super(msg);
    }
}
