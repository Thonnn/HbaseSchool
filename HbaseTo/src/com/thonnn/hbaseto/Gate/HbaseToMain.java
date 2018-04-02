package com.thonnn.hbaseto.Gate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.*;
import java.util.*;

/**
 * HbaseTo 数据表匹配导出工具
 * @author Thonnn 2017-11-26
 * @version 1.0.0
 * @since 1.0.0
 */
public class HbaseToMain {
    public static void main(String[] args) throws Exception {
        System.out.println("---------------------------------------------------------------------------");
        System.out.println("*      I want to draw a thumb here, but... sadly, I can't draw it...      *");
        System.out.println("*      You know, there is a thumb here, isn't it?                         *");
        System.out.println("*                                 Copyright: Thonnn   Date: 2017-11-26    *");
        System.out.println("---------------------------------------------------------------------------");
        String ip;
        String port;
        String tables;
        Scanner scan = new Scanner(System.in);
        System.out.print("Please input Hbase IP: ");
        ip = scan.nextLine();
        ip = ip == null || ip.equals("") ? "master1" : ip;
        System.out.print("Please input Hbase Port (Default 2181): ");
        port = scan.nextLine();
        port = port == null || port.equals("") ? "2181" : port;

        System.out.println("Tables Loading (Do Not attation to the log of 'log4j:WARN ...')......");
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientPort", port);
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        HashMap<String, List<String>> alltableMap = new HashMap<>();
        HTableDescriptor[] tableDescriptors = admin.listTables();
        System.out.println("\tGet "+ tableDescriptors.length +" tables: ");
        for (HTableDescriptor tdescriptor : tableDescriptors){
            String tablename = tdescriptor.getNameAsString();
            alltableMap.computeIfAbsent(tablename, k -> new ArrayList<>());
            List<String> list = alltableMap.get(tablename);
            for(HColumnDescriptor fdescriptor : tdescriptor.getColumnFamilies()){
                list.add(fdescriptor.getNameAsString());
            }
            System.out.println("\t\t" + tablename);
        }
        System.out.print("Please input table name ('*' is all, Use ',' to split, press 'Enter' to over.):\n\t");
        tables = scan.nextLine();
        HashMap<String, List<String>> mytableMap;
        if (!tables.trim().equals("*")){
            mytableMap = new HashMap<>();
            for(String table : tables.split(",")){
                if (alltableMap.get(table.trim()) != null){
                    mytableMap.put(table.trim(), alltableMap.get(table.trim()));
                }else {
                    System.out.println("Cannot find table of : " + table);
                    return;
                }
            }
        }else {
            mytableMap = alltableMap;
        }

        StringBuilder sbxml = new StringBuilder();
        sbxml.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n");
        sbxml.append("<HbaseGo>\n");
        Set<String> mykeySet = mytableMap.keySet();
        int taskNum = mykeySet.size();
        int count = 0;
        for(String tableKey : mykeySet){
            count++;
            String className = tableKey.substring(0, 1).toUpperCase() + tableKey.substring(1) + "Bean";
            String javaName = className + ".java";
            System.out.print("Write Loading:("+ count +"/"+ (taskNum + 1) +"): " + javaName + " ...... ");

            StringBuilder sbtable = new StringBuilder();
            sbtable.append("    <table name=\""+ tableKey +"\" bean=\"com.thonnn.hbasegotest.beans."+ className +"\">\n");
            sbtable.append("        <rowkey field=\"RowKey\" />\n");

            StringBuilder sbfield = new StringBuilder();
            sbfield.append("package com.thonnn.hbasegotest.beans;\n\n");
            sbfield.append("import com.thonnn.hbasego.interfaces.IHbaseGoBean;\n\n");
            sbfield.append("import java.io.Serializable;\n");
            sbfield.append("import java.util.HashMap;\n\n");
            sbfield.append("public class "+ className +" implements IHbaseGoBean, Serializable{\n");
            sbfield.append("    private String RowKey = null;\n");

            StringBuilder sbmethod = new StringBuilder();
            sbmethod.append("\n    public String getRowKey() {\n        return RowKey;\n    }\n\n");
            sbmethod.append("    public void setRowKey(String rowKey) {\n        RowKey = rowKey;\n    }\n\n");
            List<String> families = mytableMap.get(tableKey);
            for(String family : families){
                String methodName = family.substring(0,1).toUpperCase() + family.substring(1);
                sbtable.append("        <family name=\"" + family + "\" field=\""+ family +"\" />\n");
                sbfield.append("    private HashMap<String, Object> " + family + " = new HashMap<>();\n");
                sbmethod.append("    public HashMap<String, Object> get"+ methodName +"() {\n        return "+family+";\n    }\n\n");
                sbmethod.append("    public void set"+ methodName +"(HashMap<String, Object> "+ family +") {\n        this."+family+" = "+ family +";\n    }\n\n");

                sbmethod.append("    public Object getFrom"+ methodName +"(String "+ family +"Key) {\n        return "+family+".get("+family+"Key);\n    }\n\n");
                sbmethod.append("    public void addTo"+ methodName +"(String "+ family +"Key, Object "+ family +"Obj) {\n        "+family+".put("+ family +"Key, "+ family +"Obj);\n    }\n\n");
            }
            sbtable.append("    </table>\n");
            sbfield.append(sbmethod);
            sbfield.append("}\n");
            if(writeFile(javaName, sbfield.toString())){
                System.out.println("Successed.");
            }else {
                System.out.println("Failed.");
            }
            sbxml.append(sbtable);
        }
        sbxml.append("</HbaseGo>\n");
        System.out.print("Write Loading:("+ (count+1) +"/"+ (taskNum + 1) +"): HbaseToTables.xml ...... ");
        if(writeFile("HbaseToTables.xml", sbxml.toString())){
            System.out.println("Successed.");
        }else {
            System.out.println("Failed.");
        }
        admin.close();
        conn.close();
        System.out.println("Task Over, Write " + (taskNum + 1) + " Files To: "+ new File(HbaseToMain.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent() +" , Exit...");
        System.out.println("---------------------------------------------------------------------------");
        System.out.println("*              Separation is always sadly, see you later...               *");
        System.out.println("*                                                            By Thonnn    *");
        System.out.println("---------------------------------------------------------------------------");
    }

    private static boolean writeFile(String fileName, String content){
        String dirPath = new File(HbaseToMain.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent();
        boolean rsl = false;
        try {
            File file = new File(dirPath + "/" +fileName);
            if(file.exists()){
                file.renameTo(new File(file.getPath() + ".bak"));
            }
            file.createNewFile();
            FileWriter fileWriter = new FileWriter(file, true);
            fileWriter.write(content);
            fileWriter.close();
            rsl = true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }
}
