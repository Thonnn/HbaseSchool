package com.thonnn.hbasego;

import java.util.HashMap;

/**
 * 构造了一个 HbaseGo 的 table 映射关系类，存储一个表与一个 bean 中的数据的映射关系；<br>
 * 之中由于 bean 的类路径会存放在 HbaseGo 类的 tableBeanHashMap 中进行索引，因此本类中不再存储。
 *
 * @author Thonnn 2017-11-26
 * @version 1.2.0 将其剥离出 HbaseGo 类，开放访问权限，以支持二次开发。
 * @since 1.2.0 重命名为 HbaseGoTableMapper 并剥离出HbaseGo.java；
 * @since 1.0.0 该版本曾用名为 HbaseGoTable；
 */
public final class HbaseGoTableMapper {
    public final String tableNmae;                                  // 表名称
    public String rowkey = "RowKey";                                 // RowKey 映射及其默认值
    public final HashMap<String, String> familyMap = new HashMap<>();      // 列簇与 bean 中的字段映射存储

    /**
     * 默认构造，需要用表名创建
     * @param tableNmae 表名
     * @since 1.0.0
     */
    HbaseGoTableMapper(String tableNmae){
        this.tableNmae = tableNmae;
    }
}
