package com.thonnn.hbasego;

import com.thonnn.hbasego.exceptions.*;
import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;
import com.thonnn.hbasego.soul.HbaseGoSoulState;
import com.thonnn.hbasego.soul.HbaseGoStatusCollector;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * HbaseGo的创建者类，这个类是单实例的，且强线程安全性的，本类中需要用到大量的配置型参数，因此本类必须在用户使用本框架的任何内容之前进行初始化；<br>
 * 强制地，IP 必须配置，build() 方法必须执行且最后执行；<br>
 * 只能使用 getInstance(Class currentClass) 方法进行初始化。
 * @author Thonnn 2017-11-26
 * @version 1.2.0 增加了对日志记录器和状态收集器的汇报。
 * @since 1.0.0
 */
public final class HbaseGoBuilder {
    private static HbaseGoBuilder hbaseGoBuilder = null;        // 保证单实例使用
    private ClassLoader loader = null;                          // 当前的 ClassLoader 用于反射，以及 xml 解析
    private List<String> xmls = new ArrayList<>();             // 临时的存储了配置的xml
    private boolean built = false;                           // 是否已经创建过
    private HbaseGoBuilder(){                                    // 保证单实例

    }

    /**
     * 获取一个工厂实例，使用同步锁保证线程安全
     * @param currentClass 执行本方法的类，主要用于反射和 xml 解析
     * @return      一个工厂实例
     * @throws HbaseGoBuilderException   由于需要强线程安全性，当多个线程同时操作如果重置了 currentClass 则会出现该异常
     * @since 1.0.0
     */
    public synchronized static HbaseGoBuilder getInstance(Class currentClass) throws HbaseGoBuilderException {
        if(hbaseGoBuilder == null){
            HbaseGoStatusCollector.setHbaseGoState(HbaseGoSoulState.NOT_BUILD);
            hbaseGoBuilder = new HbaseGoBuilder();
        }
        if(hbaseGoBuilder.loader == null){
            hbaseGoBuilder.loader = currentClass.getClassLoader();
        }else{
            throw new HbaseGoBuilderException("Current Class Reset Exception.");
        }
        return hbaseGoBuilder;
    }

    /**
     * 设置 Hbase 主机 IP
     * @param ip Hbase 主机 IP
     * @return  当前工厂对象原路带回
     * @throws HbaseGoBuilderException  由于是强线程安全的，当工厂已经执行过 build() 方法之后则不再支持更改，如果尝试更改了，则会抛出该异常，同样的， IP 不能为 null
     * @since 1.0.0
     */
    public synchronized HbaseGoBuilder setIP(String ip) throws HbaseGoBuilderException {
        if(built){
            throw new HbaseGoBuilderException("HbaseGo was built, you cannot change IP again.");
        }
        if(ip == null){
            throw new HbaseGoBuilderException("IP cannot be null.");
        }
        HbaseGo.ip = ip;
        HbaseGoLoggerProxy.recordMsg(hbaseGoBuilder, HbaseGoLogType.INFO, "HbaseGoBuilder setIP: " + ip);
        return this;
    }

    /**
     * 配置 Hbase 主机端口，这个设置如果不配置则按照默认端口 2181 执行
     * @param port  端口号
     * @return  当前工厂对象原路带回
     * @throws HbaseGoBuilderException 当端口在工厂执行了 build() 方案后尝试重置时发生，当端口不符合端口标准时发生
     * @since 1.0.0
     */
    public synchronized HbaseGoBuilder setPort(int port) throws HbaseGoBuilderException {
        if(built){
            throw new HbaseGoBuilderException("HbaseGo was built, you cannot change Port again.");
        }
        if(port < 1 || port > 65535){
            throw new HbaseGoBuilderException("Port must between 1 to 65535.");
        }
        HbaseGo.port = port+"";
        HbaseGoLoggerProxy.recordMsg(hbaseGoBuilder, HbaseGoLogType.INFO, "HbaseGoBuilder setPort: " + port);
        return this;
    }

    /**
     * 配置 Hbase 的最大连接数，不配置则按照默认配置 40 执行
     * @param num   连接数
     * @return  当前工厂对象原路带回
     * @throws HbaseGoBuilderException  当端口在工厂执行了 build() 方案后尝试重置时发生，当数量小于 1 时发生
     * @since 1.0.0
     */
    public synchronized HbaseGoBuilder setMaxHbaseConnections(int num) throws HbaseGoBuilderException {
        if(built){
            throw new HbaseGoBuilderException("HbaseGo was built, you cannot change MaxHbaseConnections again.");
        }
        if(num < 1){
            throw new HbaseGoBuilderException("MaxHbaseConnections must more than 0.");
        }
        HbaseGo.maxHbaseConnectionsNum = num;
        HbaseGoLoggerProxy.recordMsg(hbaseGoBuilder, HbaseGoLogType.INFO, "HbaseGoBuilder setMaxHbaseConnections: " + num);
        return this;
    }

    /**
     * 设置初始化连接数量
     * @param num 连接数
     * @return 当前工厂对象原路带回
     * @throws HbaseGoBuilderException 当端口在工厂执行了 build() 方案后尝试重置时发生，当连接数小于 1 或者大于最大连接数时发生
     * @since 1.0.0
     */
    public synchronized HbaseGoBuilder setInitHbaseConnections(int num) throws HbaseGoBuilderException {
        if(built){
            throw new HbaseGoBuilderException("HbaseGo was built, you cannot change InitHbaseConnections again.");
        }
        if(num < 1){
            throw new HbaseGoBuilderException("InitHbaseConnections must more than 0.");
        }
        if(num > HbaseGo.maxHbaseConnectionsNum){
            throw new HbaseGoBuilderException("InitHbaseConnections must less then MaxHbaseConnections, current MaxHbaseConnections = "+ HbaseGo.maxHbaseConnectionsNum);
        }
        HbaseGo.initHbaseConnectionsNum = num;
        HbaseGoLoggerProxy.recordMsg(hbaseGoBuilder, HbaseGoLogType.INFO, "HbaseGoBuilder setInitHbaseConnections: " + num);
        return this;
    }

    /**
     * 设置连接的最大停滞时间，当因未知的错误导致连接未能正常返还时，其在 busyConnectionsList 中的最大停滞时间，超过这个时间则会自动返还 spareConnectionsQueue
     * @param seconds 时间，秒
     * @return  当前工厂对象原路带回
     * @throws HbaseGoBuilderException  当端口在工厂执行了 build() 方案后尝试重置时发生，配置时间小于 60 时发生
     * @since 1.0.0
     */
    public synchronized HbaseGoBuilder setHbaseConnectionOutTime(int seconds) throws HbaseGoBuilderException {
        if(built){
            throw new HbaseGoBuilderException("HbaseGo was built, you cannot change HbaseConnectionOutTime again.");
        }
        if(seconds < 60){
            throw new HbaseGoBuilderException("Seconds cannot be less than 60.");
        }
        HbaseGo.hbaseConnectionOutTime = seconds;
        HbaseGoLoggerProxy.recordMsg(hbaseGoBuilder, HbaseGoLogType.INFO, "HbaseGoBuilder setHbaseConnectionOutTime: " + seconds);
        return this;
    }

    /**
     * 添加准备扫描的 xml 所在的包
     * @param packageName   包名
     * @return  当前工厂对象原路带回
     * @since 1.0.0
     */
    public synchronized HbaseGoBuilder addScanPackage(String packageName){
        String packagePath = packageName.replace(".", "/");
        URL url = loader.getResource(packagePath);
        if (url != null) {
            String protocol = url.getProtocol();
            if (protocol.equals("file")) {
                for (File childFile : Objects.requireNonNull(new File(url.getPath()).listFiles())) {
                    if (!childFile.isDirectory()) {
                        String xmlPath = childFile.getPath();
                        if (xmlPath.toUpperCase().endsWith(".XML") && xmls.indexOf(xmlPath) < 0) {
                            xmls.add(xmlPath);
                        }
                    }
                }
            }
        }
        HbaseGoLoggerProxy.recordMsg(hbaseGoBuilder, HbaseGoLogType.INFO, "HbaseGoBuilder addScanPackage: " + packageName);
        return this;
    }

    /**
     * 添加准备扫描的 xml 文件路径，此路径服从 “包名.类名” 的格式，如： com.thonnn.hbasego.mappers.templete.xml
     * @param xmlDotPath    路径，服从 “包名.类名” 的格式，如： com.thonnn.hbasego.mappers.templete.xml
     * @return 当前工厂对象原路带回
     * @since 1.0.0
     */
    public synchronized HbaseGoBuilder addScanXml(String xmlDotPath){
        if (xmlDotPath.toUpperCase().endsWith(".XML")){
            String tempPath = xmlDotPath.replace(".", "/").substring(0, xmlDotPath.length() - 4) + xmlDotPath.substring(xmlDotPath.length() - 4);
            URL url = loader.getResource(tempPath);
            if (url != null) {
                String protocol = url.getProtocol();
                if (protocol.equals("file")) {
                    File file = new File(url.getPath());
                    if (file.isFile()) {
                        String xmlPath = file.getPath();
                        if (xmlPath.toUpperCase().endsWith(".XML") && xmls.indexOf(xmlPath) < 0) {
                            xmls.add(xmlPath);
                        }
                    }
                }
            }
        }
        HbaseGoLoggerProxy.recordMsg(hbaseGoBuilder, HbaseGoLogType.INFO, "HbaseGoBuilder addScanXml: " + xmlDotPath);
        return this;
    }

    /**
     * 执行初始化的最后一步 —— 创建（初始化） HbaseGo
     * @throws HbaseGoBuilderException 当尝试重新创建时发生
     * @since 1.0.0
     */
    public synchronized void build() throws HbaseGoBuilderException{
        if(!built){
            HashMap<String, HbaseGoTableMapper> tableBeanHashMap = HbaseGo.tableBeanHashMap;
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document document;
                for (String str : xmls) {                                           // 解析多个 xml
                    try{
                        File xmlFile = new File(str);
                        if(!xmlFile.exists()){
                            throw new FileNotFoundException("File not found : " + xmlFile.getPath());
                        }
                        if (!xmlFile.isFile()){
                            throw new FileNotFoundException("It is not a file : " + xmlFile.getPath());
                        }
                        document = db.parse(xmlFile);
                        Element dom = document.getDocumentElement();
                        if (!dom.getTagName().equals("HbaseGo")){
                            throw new HbaseGoBuilderException("XML format error in the file of "+ xmlFile.getPath());
                        }
                        NodeList tables = dom.getElementsByTagName("table");
                        if (tables == null || tables.getLength() <= 0){
                            throw new HbaseGoBuilderException("Element of '<table />' is necessary in file : "+ xmlFile.getPath());
                        }
                        for (int i = 0; i < tables.getLength(); i++){
                            try{
                                Node item = tables.item(i);
                                Node tableNode = item.getAttributes().getNamedItem("name");
                                if(tableNode == null || tableNode.getNodeValue().equals("")){
                                    throw new HbaseGoBuilderException("Attributes of 'name' in the " + i + "st element of '<table />' is necessary in file : "+ xmlFile.getPath());
                                }
                                Node beanNode = item.getAttributes().getNamedItem("bean");
                                if (beanNode == null || beanNode.getNodeValue().equals("")){
                                    throw new HbaseGoBuilderException("Attributes of 'bean' in the " + i + "st element of '<table />' is necessary in file : "+ xmlFile.getPath());
                                }
                                HbaseGoTableMapper hbaseGoTableMapper = new HbaseGoTableMapper(tableNode.getNodeValue());
                                NodeList rowkeys = ((Element)item).getElementsByTagName("rowkey");
                                if(rowkeys != null && rowkeys.getLength() > 0){
                                    Node field = rowkeys.item(rowkeys.getLength()-1).getAttributes().getNamedItem("field");
                                    if(field != null){
                                        hbaseGoTableMapper.rowkey = field.getNodeValue();
                                    }
                                }
                                NodeList families = ((Element)item).getElementsByTagName("family");
                                if(families == null || families.getLength() <= 0){
                                    throw new HbaseGoBuilderException("Cannot find any HbaseGo family settings in table : " + item.getAttributes().getNamedItem("name") + " in file : "+ xmlFile.getPath());
                                }
                                for (int j = 0; j < families.getLength(); j++) {
                                    try{
                                        Node settingItem = families.item(j);
                                        Node nameItem = settingItem.getAttributes().getNamedItem("name");
                                        if(nameItem == null || nameItem.getNodeValue().equals("")){
                                            throw new HbaseGoBuilderException("Attributes of 'name' in the " + j + "st element of '<family />' in table : "+ item.getAttributes().getNamedItem("name") +" is necessary in file : "+ xmlFile.getPath());
                                        }
                                        Node fieldItem = settingItem.getAttributes().getNamedItem("field");
                                        if(fieldItem == null || fieldItem.getNodeValue().equals("")){
                                            throw new HbaseGoBuilderException("Attributes of 'field' in the "+ j +"st element of '<family />'  in table : "+ item.getAttributes().getNamedItem("name") +" is necessary in file : "+ xmlFile.getPath());
                                        }
                                        if (hbaseGoTableMapper.familyMap.containsKey(nameItem.getNodeValue())){
                                            throw new HbaseGoBuilderException("It's already exists an identical name family of '"+ nameItem.getNodeValue() +"' in file : "+ xmlFile.getPath());
                                        }
                                        hbaseGoTableMapper.familyMap.put(nameItem.getNodeValue(), fieldItem.getNodeValue());
                                    }catch (HbaseGoBuilderException e){
                                        e.printStackTrace();
                                    }
                                }
                                if(tableBeanHashMap.containsKey(hbaseGoTableMapper.tableNmae)){
                                    throw new HbaseGoBuilderException("It's already exists an identical name table of '"+ hbaseGoTableMapper.tableNmae +"' in file : "+ xmlFile.getPath());
                                }
                                tableBeanHashMap.put(beanNode.getNodeValue(), hbaseGoTableMapper);
                            }catch (HbaseGoBuilderException e){
                                e.printStackTrace();
                            }
                        }
                    }catch (SAXException|IOException|HbaseGoBuilderException e){
                        e.printStackTrace();
                    }
                }
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }
            try {
                HbaseGo.connectHbase();
            } catch (HbaseGoSelfException e) {
                e.printStackTrace();
            }
            built = true;
            HbaseGoStatusCollector.setHbaseGoState(HbaseGoSoulState.BUILT);
            HbaseGoLoggerProxy.recordMsg(hbaseGoBuilder, HbaseGoLogType.INFO, "HbaseGoBuilder built successfully.");
            return;
        }
        throw new HbaseGoBuilderException("HbaseGo re-build exception.");
    }
}
