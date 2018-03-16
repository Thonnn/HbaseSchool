package com.thonnn.hbasegotest.beans;

import com.thonnn.hbasego.interfaces.IHbaseGoBean;

import java.io.Serializable;
import java.util.HashMap;

public class Test1Bean implements IHbaseGoBean, Serializable{
    private String RowKey = null;
    private HashMap<String, Object> name = new HashMap<>();
    private HashMap<String, Object> other = new HashMap<>();
    private HashMap<String, Object> sex = new HashMap<>();

    public String getRowKey() {
        return RowKey;
    }

    public void setRowKey(String rowKey) {
        RowKey = rowKey;
    }

    public HashMap<String, Object> getName() {
        return name;
    }

    public void setName(HashMap<String, Object> name) {
        this.name = name;
    }

    public Object getFromName(String nameKey) {
        return name.get(nameKey);
    }

    public void addToName(String nameKey, Object nameObj) {
        name.put(nameKey, nameObj);
    }

    public HashMap<String, Object> getOther() {
        return other;
    }

    public void setOther(HashMap<String, Object> other) {
        this.other = other;
    }

    public Object getFromOther(String otherKey) {
        return other.get(otherKey);
    }

    public void addToOther(String otherKey, Object otherObj) {
        other.put(otherKey, otherObj);
    }

    public HashMap<String, Object> getSex() {
        return sex;
    }

    public void setSex(HashMap<String, Object> sex) {
        this.sex = sex;
    }

    public Object getFromSex(String sexKey) {
        return sex.get(sexKey);
    }

    public void addToSex(String sexKey, Object sexObj) {
        sex.put(sexKey, sexObj);
    }

}
