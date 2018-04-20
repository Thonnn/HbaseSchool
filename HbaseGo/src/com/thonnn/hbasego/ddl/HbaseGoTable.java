package com.thonnn.hbasego.ddl;

import com.thonnn.hbasego.exceptions.HbaseGoDDLException;

import java.util.ArrayList;
import java.util.List;

/**
 * 构造的用于创建 Hbase 表的类。
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public final class HbaseGoTable {
    private String tableName;
    private List<HbaseGoTableFamily> families = new ArrayList<>();

    /**
     * 使用表名称进行构造
     * @param tableName 表名称
     * @since 1.2.0
     */
    public HbaseGoTable(String tableName){
        this.tableName = tableName;
    }

    /**
     * 添加一个列簇
     * @param family 列簇
     * @return 当前对象原路带回
     * @since 1.2.0
     */
    public HbaseGoTable addFamily(HbaseGoTableFamily family){
        for (HbaseGoTableFamily f : families){
            if (f.NAME.equals(family.NAME)){
                try {
                    throw new HbaseGoDDLException("Exist a Family with the name of '" + family.NAME +"' in table '" + tableName + "'.");
                } catch (HbaseGoDDLException e) {
                    e.printStackTrace();
                }
                return this;
            }
        }
        families.add(family);
        return this;
    }

    /**
     * 获取表名称
     * @return 表名称
     */
    public String getTableName(){
        return tableName;
    }

    /**
     * 获取已经配置的列簇的列表
     * @return 已经配置的列簇的列表
     */
    public List<HbaseGoTableFamily> getFamiliesList(){
        return families;
    }
}
