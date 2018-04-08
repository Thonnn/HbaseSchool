package com.thonnn.hbasegotest;

import com.thonnn.hbasego.assists.HbaseGoVersionBean;
import com.thonnn.hbasego.dao.HbaseGoBuilder;
import com.thonnn.hbasego.dao.HbaseGoToTableDAO;
import com.thonnn.hbasego.utils.IDUtil;
import com.thonnn.hbasegotest.beans.CjdxBean;

import java.util.List;

public class HbaseGoTestMain {
    public static void main(String[] args) throws Exception {
        String packageName = "com.thonnn.hbasegotest.mappers";
        HbaseGoBuilder hbaseGoBuilder = HbaseGoBuilder.getInstance(HbaseGoTestMain.class).addScanPackage(packageName).setIP("master1");
        hbaseGoBuilder.build();
        HbaseGoToTableDAO dao = new HbaseGoToTableDAO();
        IDUtil.setTailLength(5);
        //------------------------Test 1--------------------------------------------------------------------------------
        System.out.println();
        CjdxBean cjdxBean_1 = new CjdxBean();
        cjdxBean_1.addToClasses("test1", "test1 Ban");
        cjdxBean_1.addToStudents("student101", "Cat1");
        cjdxBean_1.addToStudents("student102", "Dog1");
        cjdxBean_1.addToStudents("student103", "Tiger1");
        System.out.println("---> Add cjdxBean_1: "+dao.add(cjdxBean_1));

        CjdxBean cjdxBean_2 = new CjdxBean();
        cjdxBean_2.addToClasses("test2", "test2 Ban");
        cjdxBean_2.addToStudents("student201", "Cat2");
        cjdxBean_2.addToStudents("student202", "Dog2");
        cjdxBean_2.addToStudents("student203", "Tiger2");
        System.out.println("---> Add cjdxBean_2: "+dao.add(cjdxBean_2));

        List<CjdxBean> rsl1 = dao.search(cjdxBean_1);
        System.out.println("---> search cjdxBean_1 rsl.size = " + rsl1.size());
        cjdxBean_1 = rsl1.get(0);
        System.out.println("---> cjdxBean_1.rowkey = " + cjdxBean_1.getRowKey());

        rsl1 = dao.search(cjdxBean_2);
        System.out.println("---> search cjdxBean_2 rsl.size = " + rsl1.size());
        cjdxBean_2 = rsl1.get(0);
        System.out.println("---> cjdxBean_2.rowkey = " + cjdxBean_2.getRowKey());

        cjdxBean_1.addToStudents("student101", "Cat1 Alter");
        System.out.println("---> Alter cjdxBean_1: "+dao.alter(cjdxBean_1));

        rsl1 = dao.search(cjdxBean_1);
        System.out.println("---> re-search cjdxBean_1 rsl.size = " + rsl1.size());
        //------------------------Test 2--------------------------------------------------------------------------------
        System.out.println();
        List<HbaseGoVersionBean<CjdxBean>> rsl2 = dao.searchByRowKeyRange(CjdxBean.class, cjdxBean_1.getRowKey(), cjdxBean_1.getRowKey(), -1);
        System.out.println("===> searchRangeCount = " + dao.searchRowKeyRangeCount(CjdxBean.class, cjdxBean_1.getRowKey(), cjdxBean_1.getRowKey()));
        System.out.println("===> rsl.size = " + rsl2.size());
        for (HbaseGoVersionBean<CjdxBean> hgvb : rsl2){
            System.out.println("===> hgvb.getVersionsNum = " + hgvb.getVersionsNum());
            List<CjdxBean> list = hgvb.getList();
            for (CjdxBean cb : list) {
                System.out.println("**********************************************");
                System.out.println("===> cb.getRowKey = " + cb.getRowKey());
                System.out.println("===> cb.getClasses = " + cb.getClasses());
                System.out.println("===> cb.getStudents = " + cb.getStudents());
            }
        }
        System.out.println("**********************************************");
        System.out.println("===> firstTimestamp = "+rsl2.get(0).getTimestampByBean(rsl2.get(0).getFirstVersionBean()));
        System.out.println("===> lastTimestamp  = " + rsl2.get(0).getTimestampByBean(rsl2.get(0).getLastVersionBean()));
        //------------------------Test 3--------------------------------------------------------------------------------
        System.out.println();
        CjdxBean cbb = new CjdxBean();
        cbb.setRowKey(cjdxBean_1.getRowKey());
        System.out.println("===> searchCount = " + dao.searchCount(cbb));
        List<HbaseGoVersionBean<CjdxBean>> rsl = dao.search(cbb, -1);
        System.out.println("***> rsl.size = " + rsl.size());
        for (HbaseGoVersionBean<CjdxBean> hgvb : rsl){
            System.out.println("***> hgvb.getVersionsNum = " + hgvb.getVersionsNum());
            List<CjdxBean> list = hgvb.getList();
            for (CjdxBean cb : list) {
                System.out.println("**********************************************");
                System.out.println("***> cb.getRowKey = " + cb.getRowKey());
                System.out.println("***> cb.getClasses = " + cb.getClasses());
                System.out.println("***> cb.getStudents = " + cb.getStudents());
            }
        }
        System.out.println("**********************************************");
        System.out.println("---> firstTimestamp = "+rsl.get(0).getTimestampByBean(rsl.get(0).getFirstVersionBean()));
        System.out.println("---> lastTimestamp  = " + rsl.get(0).getTimestampByBean(rsl.get(0).getLastVersionBean()));

        System.exit(0);
    }
}