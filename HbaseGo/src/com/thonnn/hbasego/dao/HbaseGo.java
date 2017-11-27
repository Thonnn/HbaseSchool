package com.thonnn.hbasego.dao;

import com.thonnn.hbasego.exceptions.HbaseConnectException;
import com.thonnn.hbasego.exceptions.OutOfMaxHbaseConnectonsNumException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

/**
 * HbaseGo 类为 HbaseGoToTableDAO 类的依赖类，本类为默认 Default 包访问权限；
 * 本类只能静态访问，所有内容都为保护成员和私有成员，此类不向外开放；
 * @author Thonnn 2017-11-26
 */
class HbaseGo {

    protected static HashMap<String, HbaseGoTable> tableBeanHashMap = null;                     // 一个存储了 Hbase 表与 bean映射关系的 HashMap
    protected static String ip = null;                                                          // Hbase 的 IP
    protected static String port = "2181";                                                      // Hbase 的端口
    protected static int maxHbaseConnectionsNum = 40;                                           // Hbase 的最大连接数
    protected static int initHbaseConnectionsNum = 5;                                           // 连接初始化时建立的连接数
    protected static int hbaseConnectionOutTime = 300;                                          // 连接最大停滞时间，当因未知的错误导致连接未能正常返还时，其在 busyConnectionsList 中的最大停滞时间，超过这个时间则会自动返还 spareConnectionsQueue

    private static Configuration conf = HBaseConfiguration.create();                            // Hbase 连接配置
    private static Queue<Connection> spareConnectionsQueue = new LinkedList<>();      // 空闲的连接队列
    private static List<Connection> busyConnectionsList = new ArrayList<>();          // 繁忙中的连接列表
    private static List<MyInt> busyConnectionsTimes = new ArrayList<>();                   // 繁忙中的连接的停滞时间列表，其类型参数，MyInt 类为构造的一个用于引用型传递的类

    private HbaseGo(){}

    /**
     * 获取一个 Hbase 连接，使用同步锁保证连接的获取不会发生冲突和其他线程安全问题
     * @param safely 是否使用安全性获取，安全获取是获取一个不会冲突的的连接，但当连接数达到上限时会出现 OutOfMaxHbaseConnectonsNumException ；
     *               如果此值为 false，当连接超过超过配置的最大上限时会不安全地从 busyConnectionsList 的0位置获取一个连接；
     *               默认为 true。
     * @return  一个 Hbase 连接实例
     */
    protected synchronized static Connection getHbaseConnection(boolean safely){
        Connection conn = spareConnectionsQueue.poll();
        if(conn == null){
            if(busyConnectionsList.size() >= maxHbaseConnectionsNum){
                if(safely){     // 安全地获取
                    try {
                        throw new OutOfMaxHbaseConnectonsNumException();
                    } catch (OutOfMaxHbaseConnectonsNumException e) {
                        e.printStackTrace();
                    }
                }else {         // 不安全地获取
                    conn = busyConnectionsList.get(0);
                }
            }else{
                conn = getHbaseConnection();
            }
        }
        busyConnectionsList.add(conn);
        busyConnectionsTimes.add(new MyInt());      // 监控停滞时间
        return conn;
    }

    /**
     * 返还 Hbase 连接，从繁忙列表中移除，并加入空闲队列，同时消除其对应的停滞时间
     * @param connection 期望返还的连接
     */
    protected synchronized static void flybackConnection(Connection connection){
        int index = busyConnectionsList.indexOf(connection);
        busyConnectionsList.remove(connection);
        busyConnectionsTimes.remove(index);         // 消除停滞时间
        spareConnectionsQueue.offer(connection);
    }

    /**
     * 创建一个 Hbase 连接
     * @return 一个连接
     */
    private static Connection getHbaseConnection(){
        try {
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 连接到 Hbase，相当于初始化连接，方法内部会创建一个自管理线程，防止因不可预知地错误/异常导致 Hbase 连接长时间停滞在繁忙列表中
     * @throws HbaseConnectException 一个属于 HbaseGo 的 Hbase 连接异常
     */
    protected static void connectHbase() throws HbaseConnectException {
        if (ip == null){
            throw new HbaseConnectException("IP cannot be null...");
        }
        if(port == null){
            throw new HbaseConnectException("Port cannot be null...");
        }
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientPort", port);
        for(int i = 0; i < initHbaseConnectionsNum; i++){
            spareConnectionsQueue.offer(getHbaseConnection());
        }

        // Hbase Connection 自管理连接线程
        new Thread(() -> {
            while (true){
                try {                                                           // 本 try 的目的是为了保证线程不会因未知错误中断
                    int length = busyConnectionsTimes.size();
                    for(int i = 0; i < length; i++){
                        if(busyConnectionsTimes.get(i).value >= hbaseConnectionOutTime){
                            flybackConnection(busyConnectionsList.get(i));      // 停滞时间达到上限自回收
                            i--;                                                // 必须使 i - 1
                            length--;                                           // 必须使 length - 1
                            System.out.println("====> Auto recycle a out-time Hbase connection.");
                        }else {
                            busyConnectionsTimes.get(i).value++;                // 停滞时间未达到上限，使其停滞时间增加
                        }
                    }
                    Thread.sleep(1000);                                   // 线程休眠
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }


}

/**
 * 构造了一个 HbaseGo 的 table 类，存储一个表与一个 bean 中的数据的映射关系；
 * 之中由于 bean 的类路径会存放在 HbaseGo 类的 tableBeanHashMap 中进行索引，因此本类中不在存储。
 *
 * @author Thonnn 2017-11-26
 */
class HbaseGoTable{
    protected String tableNmae;                                  // 表名称
    protected String rowkey = "RowKey";                                 // RowKey 映射及其默认值
    protected HashMap<String, String> familyMap = new HashMap<>();      // 列簇与 bean 中的字段映射存储
    HbaseGoTable(String tableNmae){                                        // 默认构造，需要用表名创建
        this.tableNmae = tableNmae;
    }
}

/**
 * 一个用于管理停滞时间的类
 *
 * @author Thonnn 2017-11-26
 */
class MyInt{
    protected int value = 0;

    @Override
    public String toString() {
        return value+"";
    }
}
