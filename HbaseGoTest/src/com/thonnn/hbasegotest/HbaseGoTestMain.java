package com.thonnn.hbasegotest;

import com.thonnn.hbasego.HbaseGoBuilder;
import com.thonnn.hbasego.dao.HbaseGoDAO;
import com.thonnn.hbasego.dao.HbaseGoVersionBean;
import com.thonnn.hbasego.ddl.HbaseGoDDL;
import com.thonnn.hbasego.ddl.HbaseGoTable;
import com.thonnn.hbasego.ddl.HbaseGoTableFamily;
import com.thonnn.hbasego.interfaces.IHbaseGoBean;
import com.thonnn.hbasego.logger.*;
import com.thonnn.hbasego.soul.HbaseGoStatusCollector;
import com.thonnn.hbasegotest.beans.CjdxBean;

import java.util.ArrayList;
import java.util.List;

public class HbaseGoTestMain {
    public static void main(String[] args) throws Exception {
        String packageName = "com.thonnn.hbasegotest.mappers";
        //------初始化配置----------------------------------------------------------------------------------------------
        HbaseGoBuilder hbaseGoBuilder = HbaseGoBuilder.getInstance(HbaseGoTestMain.class).addScanPackage(packageName).setIP("master1").setHbaseConnectionOutTime(60);
        hbaseGoBuilder.build();
        System.out.println("==> 初始化完成");
        //------创建默认日志记录器并启用，同时添加日志打印机------------------------------------------------------------------
        HbaseGoDefaultLogger logger = new HbaseGoDefaultLogger();
        HbaseGoDefaultLogPrinter printer = new HbaseGoDefaultLogPrinter();
        logger.setLogPrinter(printer);
        logger.setMaxCacheSize(100);
        logger.setLogFileDir("C:/Users/Thonn/Desktop/HbaseGoLog/logs/");
        logger.startWrite();
        HbaseGoLoggerProxy.setLogger(logger);
        System.out.println("==> 添加日志记录器完成");
        //------创建DAO-------------------------------------------------------------------------------------------------
        HbaseGoDAO dao = new HbaseGoDAO();
        //------创建DDL-------------------------------------------------------------------------------------------------
        HbaseGoDDL ddl = new HbaseGoDDL();
        //------以下是 DDL 演示------------------------------------------------------------------------------------------
        //----------创建表 “javaTable”，带两个列簇 “family1” 和 “family2”-------------------------------------------
        HbaseGoTable javaTable = new HbaseGoTable("javaTable");
        HbaseGoTableFamily family1 = new HbaseGoTableFamily("family1");
        //----------使用 family1 克隆出 family2，并设置其 MAX_VERSIONS 为3 ----------------------------------------------
        HbaseGoTableFamily family2 = family1.cloneThis("family2");
        family2.MAX_VERSIONS = 3;
        //----------使用 ddl 创建这张表----------------------------------------------------------------------------------
        javaTable.addFamily(family1).addFamily(family2);
        System.out.println("==> javaTable 是否存在: " + ddl.existTable(javaTable.getTableName()));
        System.out.println("==> 创建 javaTable: " + ddl.createTable(javaTable));
        //----------为 javaTable 添加一个 列簇---------------------------------------------------------------------------
        System.out.println("==> 添加一个列簇结果: " + ddl.addFamily(javaTable.getTableName(), new HbaseGoTableFamily("family_add1")));
        //----------为 javaTable 删除 family1---------------------------------------------------------------------------
        System.out.println("==> 删除family1: "+ ddl.deleteFamily(javaTable.getTableName(), "family1"));
        //----------将 family2 的 MIN_VERSIONS 设置为 2 ----------------------------------------------------------------
        family2.MIN_VERSIONS = 2;
        System.out.println("==> 修改family2: "+ ddl.alterFamily(javaTable.getTableName(), family2));
        //----------尝试删除一张不存在的表-------------------------------------------------------------------------------
        System.out.println("==> 删除不存在的表：" + ddl.deleteTable("NONE"));

        //------以下是 DAO 演示------------------------------------------------------------------------------------------
        //----------向表 cjdx 中添加一条数据-----------------------------------------------------------------------------
        CjdxBean cjdxBean1 = new CjdxBean();
        cjdxBean1.setSex("boy");
        System.out.println("==> 添加一条数据：" + dao.add(cjdxBean1));
        //----------向表 cjdx 中添加 一个list----------------------------------------------------------------------------
        CjdxBean cjdxBean2 = new CjdxBean();
        cjdxBean2.setSex("girl");
        CjdxBean cjdxBean3 = new CjdxBean();
        cjdxBean3.setSex("other");
        List<IHbaseGoBean> list = new ArrayList<>();
        list.add(cjdxBean2);
        list.add(cjdxBean3);
        System.out.println("==> 添加一个列表：" + dao.add(list));
        //----------从 cjdx 中批量删除 cjdxBean1 和 cjdxBean2 -----------------------------------------------------------
        list.clear();
        list.add(cjdxBean1);
        list.add(cjdxBean2);
        System.out.println("==> 删除cjdxBean1,2：" + dao.delete(list));
        //----------修改 cjdxBean3 的 sex 为 ‘alter’ ------------------------------------------------------------------
        System.out.println("==> cjdxBean3.rowkey: " +cjdxBean3.getRowKey());
        cjdxBean3.setSex("alter");
        System.out.println("==> 修改cjdxBean3：" + dao.alter(cjdxBean3));
        //----------根据cjdxBean3 的rowkey查询其所有版本-----------------------------------------------------------------
        List<HbaseGoVersionBean<CjdxBean>> versionBeanList = dao.search(cjdxBean3, -1);
        System.out.println("==> 多版本查询结果数：" + versionBeanList.get(0).getList().size());

        //-------以下是日志记录器的操作----------------------------------------------------------------------------------
        //-----------停止日志记录器写文件--------------------------------------------------------------------------------
        logger.stopWrite();
        System.out.println("==> 已停止日志记录器写文件");
        //-----------取消日志打印机--------------------------------------------------------------------------------------
        logger.setLogPrinter(null);
        System.out.println("==> 已取消日志打印机");
        //-----------取消日志记录器--------------------------------------------------------------------------------------
        HbaseGoLoggerProxy.setLogger(null);
        System.out.println("==> 已取消日志记录器");
        //-----------为日志记录器添加一个自定义的日志记录器---------------------------------------------------------------
        HbaseGoLoggerProxy.setLogger(new IHbaseGoLogger(){

            @Override
            public void recordMsg(Object currentObject, HbaseGoLogType type, String msg) {
                System.out.println("***> MyLogger: " + msg);
            }
        });
        //-----------测试自定义日志记录器是否生效-------------------------------------------------------------------------
        System.out.println("==> javaTable 存在：" + ddl.existTable("javaTable"));

        //------以下测试 HbaseGoStatusCollector 信息---------------------------------------------------------------------
        System.out.println("==> getHbaseGoState：" +HbaseGoStatusCollector.getHbaseGoState());
        System.out.println("==> getBusyConnectionsNum：" +HbaseGoStatusCollector.getBusyConnectionsNum());
        System.out.println("==> getSpareConnectionsNum：" +HbaseGoStatusCollector.getSpareConnectionsNum());
        System.out.println("==> getBusyConnectionsTimesListSize：" +HbaseGoStatusCollector.getBusyConnectionsTimesListSize());
        System.out.println("==> getSelfManagingThreadState：" +HbaseGoStatusCollector.getSelfManagingThreadState());
        //System.exit(0);
    }
}