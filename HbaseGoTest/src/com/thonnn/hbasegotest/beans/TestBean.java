package com.thonnn.hbasegotest.beans;

import com.thonnn.hbasego.interfaces.IHbaseGoBean;

import java.io.Serializable;
import java.util.HashMap;

public class TestBean implements IHbaseGoBean, Serializable{
    private String RowKey = null;
    private HashMap<String, Object> f1 = new HashMap<>();
    private HashMap<String, Object> f2 = new HashMap<>();

    public void addToF1(String key, Object value){
        f1.put(key, value);
    }
    public Object getFromF1(String key){
        return f1.get(key);
    }

    public void addToF2(String key, Object value){
        f2.put(key, value);
    }
    public Object getFromF2(String key){
        return f2.get(key);
    }

    public HashMap<String, Object> getF1() {
        return f1;
    }

    public void setF1(HashMap<String, Object> f1) {
        this.f1 = f1;
    }

    public HashMap<String, Object> getF2() {
        return f2;
    }

    public void setF2(HashMap<String, Object> f2) {
        this.f2 = f2;
    }

    public String getRowKey() {
        return RowKey;
    }

    public void setRowKey(String rowKey) {
        RowKey = rowKey;
    }
}
