package com.thonnn.hbasegotest.beans;

import com.thonnn.hbasego.interfaces.IHbaseGoBean;

import java.io.Serializable;
import java.util.HashMap;

public class CjdxBean implements IHbaseGoBean, Serializable{
    private String RowKey = null;
    private HashMap<String, Object> classes = new HashMap<>();
    private HashMap<String, Object> students = new HashMap<>();

    public String getRowKey() {
        return RowKey;
    }

    public void setRowKey(String rowKey) {
        RowKey = rowKey;
    }

    public void setSex(String sex){
        students.put("ZhangSan", sex);
    }

    public void getSex(){
        students.get("ZhangSan");
    }

    public HashMap<String, Object> getClasses() {
        return classes;
    }

    public void setClasses(HashMap<String, Object> classes) {
        this.classes = classes;
    }

    public Object getFromClasses(String classesKey) {
        return classes.get(classesKey);
    }

    public void addToClasses(String classesKey, Object classesObj) {
        classes.put(classesKey, classesObj);
    }

    public HashMap<String, Object> getStudents() {
        return students;
    }

    public void setStudents(HashMap<String, Object> students) {
        this.students = students;
    }

    public Object getFromStudents(String studentsKey) {
        return students.get(studentsKey);
    }

    public void addToStudents(String studentsKey, Object studentsObj) {
        students.put(studentsKey, studentsObj);
    }

}
