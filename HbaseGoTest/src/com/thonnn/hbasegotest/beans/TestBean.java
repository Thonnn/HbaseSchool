package com.thonnn.hbasegotest.beans;

import com.thonnn.hbasego.interfaces.IHbaseGoBean;

import java.io.Serializable;
import java.util.HashMap;

public class TestBean implements IHbaseGoBean, Serializable{
    private static final long serialVersionUID = 1L;
    private String RowKey = null;
    private HashMap<String, Object> basic_msg = new HashMap<>();
    private HashMap<String, Object> feature_data = new HashMap<>();
    private HashMap<String, Object> other = new HashMap<>();

    public String getRowKey() {
        return RowKey;
    }

    public void setRowKey(String rowKey) {
        RowKey = rowKey;
    }

    public String getName(){
        Object obj = basic_msg.get("name");
        if(obj == null){
            return null;
        }
        return (String)obj;
    }

    public void setName(String name){
        basic_msg.put("name", name);
    }

    public int getAge(){
        Object obj = basic_msg.get("age");
        if(obj == null){
            return -1;
        }
        return (int)obj;
    }

    public void setAge(int age){
        basic_msg.put("age", age);
    }

    public int getSex(){
        Object obj = basic_msg.get("sex");
        if(obj == null){
            return -1;
        }
        return (int)obj;
    }

    public void setSex(int sex){
        basic_msg.put("sex", sex);
    }

    public char[] getFeature(){
        Object obj = feature_data.get("feature");
        if(obj == null){
            return null;
        }
        return (char[])obj;
    }

    public void setFeature(char[] feature){
        feature_data.put("feature", feature);
    }

    public String getPhotoPath(){
        Object obj = feature_data.get("photo_path");
        if(obj == null){
            return null;
        }
        return (String)obj;
    }

    public void setPhotoPath(String photo_path){
        feature_data.put("photo_path", photo_path);
    }

    public float getA(){
        Object obj = other.get("A");
        if(obj == null){
            return -1;
        }
        return (float)obj;
    }

    public void setA(float A){
        other.put("A", A);
    }

    public float getB(){
        Object obj = other.get("B");
        if(obj == null){
            return -1;
        }
        return (float)obj;
    }

    public void setB(float B){
        other.put("B", B);
    }

    public float getC(){
        Object obj = other.get("C");
        if(obj == null){
            return -1;
        }
        return (float)obj;
    }

    public void setC(float C){
        other.put("C", C);
    }

    public float getD(){
        Object obj = other.get("D");
        if(obj == null){
            return -1;
        }
        return (float)obj;
    }

    public void setD(float D){
        other.put("D", D);
    }
}
