package com.thonnn.hbasegotest.beans;

import com.thonnn.hbasego.interfaces.IHbaseGoBean;

import java.io.Serializable;
import java.util.HashMap;

public class Test2Bean implements IHbaseGoBean, Serializable{
    public String id = null;
    private String RowKey = null;
    private HashMap<String, Object> f21 = new HashMap<>();
    private HashMap<String, Object> f22 = new HashMap<>();

    public String getRowKey() {
        return RowKey;
    }

    public void setRowKey(String rowKey) {
        RowKey = rowKey;
    }

    public HashMap<String, Object> getF21() {
        return f21;
    }

    public void setF21(HashMap<String, Object> f21) {
        this.f21 = f21;
    }

    public HashMap<String, Object> getF22() {
        return f22;
    }

    public void setF22(HashMap<String, Object> f22) {
        this.f22 = f22;
    }
}
