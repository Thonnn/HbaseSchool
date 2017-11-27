package com.thonnn.hbasegotest;

import com.thonnn.hbasego.dao.HbaseGoBuilder;
import com.thonnn.hbasego.dao.HbaseGoToTableDAO;
import com.thonnn.hbasego.interfaces.IHbaseGoBean;
import com.thonnn.hbasego.utils.IDUtil;
import com.thonnn.hbasegotest.beans.Test2Bean;
import com.thonnn.hbasegotest.beans.TestBean;

import javax.naming.Context;
import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class HbaseGoTestMain {
    public static void main(String[] args) throws Exception {
        String packageName = "com.thonnn.hbasegotest.mappers";
        HbaseGoBuilder hbaseGoBuilder = HbaseGoBuilder.getInstance(new HbaseGoTestMain().getClass()).addScanPackage(packageName).setIP("master1");
        hbaseGoBuilder.build();
        TestBean tb = new TestBean();
        //-----------ADD---------------------------------------------------------
        /*tb.addToF1("tb11", "tb11");
        tb.addToF1("tb12", "tb12");
        tb.addToF2("tb21", "tb21");
        tb.addToF2("tb22", "tb22");
        tb.addToF1(null, "tb33");

        TestBean tb2 = new TestBean();
        tb2.addToF1("tb11", "tb11");
        tb2.addToF1("tb12", "tb12");
        tb2.addToF2("tb21", "tb21");
        tb2.addToF2("tb22", "tb22");
        tb2.addToF1(null, "tb33");*/

        /**Test2Bean t2b = new Test2Bean();
        t2b.id = "888";
        t2b.getF22().put("t2b1", "t2b1");
        t2b.getF22().put("t2b2", "t2b2");
        t2b.getF22().put(null, "t2b null");
        t2b.getF22().put("", "t2b ''");

        Test2Bean t2b2 = new Test2Bean();
        t2b2.getF22().put("t2b21", "t2b21");
        t2b2.getF22().put("t2b22", "t2b22");
        t2b2.getF22().put(null, "t2b2 null");
        t2b2.getF22().put("", "t2b2 ''");
        */
        /*List<IHbaseGoBean> list = new ArrayList<>();
        list.add(tb);
        list.add(tb2);*/
        /*
        list.add(t2b);
        list.add(t2b2);
        */
        tb.addToF1("tb12", "tb12");
        tb.setF2(null);
        List<IHbaseGoBean> list = new HbaseGoToTableDAO().search(tb);
        System.out.println(list.size());
        for(IHbaseGoBean tbb : list){
            TestBean tt = (TestBean) tbb;
            System.out.println(tt.getF1());
            System.out.println(tt.getF2());
        }

        //-----------Search---------------------------------------------------------
        //tb.setRowKey("201711252307201815867");
        /*tb.addToF1("11", "11");
        TestBean tb2 = (TestBean) new HbaseGoToTableDAO().search(tb).get(0);
        System.out.println(tb.getF1());
        System.out.println(tb.getF2());
        System.out.println(tb2.getF1());
        System.out.println(tb2.getF2());
        System.out.println(new HbaseGoToTableDAO().searchCount(tb));*/
        //----------Alter----------------------------------------------------------------
        /*tb.addToF1("tb11", "991");
        tb.addToF1("tb12", "991");
        tb.addToF2("tb21", "991");
        tb.addToF2("tb22", "991");
        tb.addToF1(null, "991");
        tb.setRowKey("222");
        HbaseGoToTableDAO hbaseGoToTableDAO = new HbaseGoToTableDAO(true, true);
        System.out.println(hbaseGoToTableDAO.alter(tb));*/
        //----------delete----------------------------------------------------------------
        //tb.setRowKey("222");
        //System.out.println(new HbaseGoToTableDAO(true, true).delete(tb));
    }
}