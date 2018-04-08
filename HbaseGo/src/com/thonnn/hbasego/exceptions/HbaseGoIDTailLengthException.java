package com.thonnn.hbasego.exceptions;

public class HbaseGoIDTailLengthException extends Exception {
    public HbaseGoIDTailLengthException(){
        super("HbaseGo ID Tail Length mast in range of [4, 10].");
    }
    public HbaseGoIDTailLengthException(String msg){
        super(msg);
    }
}
