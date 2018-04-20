package com.thonnn.hbasego;

import com.thonnn.hbasego.exceptions.HbaseGoSelfException;
import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;
import com.thonnn.hbasego.soul.HbaseGoSoulState;
import com.thonnn.hbasego.soul.HbaseGoStatusCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.*;

/**
 * HbaseGo 类为 HbaseGoDAO 和 HbaseGoDDL 的依赖类；<br>
 * 从 v1.2.0 开始开放了本类的访问权限，现在也许你可以用这其中已经封装好的线程池等资源，进行自主开发了。<br>
 * 如果你尝试使用本类进行自主开发，请牢记，你使用 getHbaseConnection() 获取的连接都应在使用结束后使用 flybackConnection() 方法放回；<br>
 * 本类只能静态访问。
 * @author Thonnn 2017-11-26
 * @version 1.2.0 增加了对日志记录器和状态收集器的汇报。
 * @since 1.0.0
 */
public final class HbaseGo {

    public static final HashMap<String, HbaseGoTableMapper> tableBeanHashMap = new HashMap<>();       // 一个存储了 Hbase 表与 bean映射关系的 HashMap
    static String ip = null;                                                          // Hbase 的 IP
    static String port = "2181";                                                      // Hbase 的端口
    static int maxHbaseConnectionsNum = 40;                                           // Hbase 的最大连接数
    static int initHbaseConnectionsNum = 5;                                           // 连接初始化时建立的连接数
    static int hbaseConnectionOutTime = 300;                                          // 连接最大停滞时间，当因未知的错误导致连接未能正常返还时，其在 busyConnectionsList 中的最大停滞时间，超过这个时间则会自动返还 spareConnectionsQueue,
                                                                                       // 这个时间还将用于当工程不活跃时间超过其4倍数时，自动释放掉所有连接。
    private static boolean stopSelfManagerFlag = false;                             // 自管理线程停止运行信号
    private static Configuration conf = HBaseConfiguration.create();                  // Hbase 连接配置
    private static Queue<Connection> spareConnectionsQueue = new LinkedList<>();      // 空闲的连接队列
    private static List<Connection> busyConnectionsList = new ArrayList<>();          // 繁忙中的连接列表
    private static List<MyInt> busyConnectionsTimes = new ArrayList<>();              // 繁忙中的连接的停滞时间列表，其类型参数，MyInt 类为构造的一个用于引用型传递的类
    private static long stagnationClock = System.currentTimeMillis();
    private HbaseGo(){}

    /**
     * 获取一个 Hbase 连接，使用同步锁保证连接的获取不会发生冲突和其他线程安全问题。
     * @param safely 是否使用安全性获取，安全获取是获取一个不会冲突的的连接，但当连接数达到上限时会出现 HbaseGoSelfException ；<br>
     *               如果此值为 false，当连接超过超过配置的最大上限时会不安全地从 busyConnectionsList 的0位置获取一个连接；<br>
     *               默认为 true。
     * @return  一个 Hbase 连接实例
     * @since 1.0.0
     */
    public synchronized static Connection getHbaseConnection(boolean safely){
        Connection conn = spareConnectionsQueue.poll();
        if(conn == null){
            if(busyConnectionsList.size() >= maxHbaseConnectionsNum){
                if(safely){     // 安全地获取
                    try {
                        throw new HbaseGoSelfException("Out of maxHbaseConnectionsNun Exception.");
                    } catch (HbaseGoSelfException e) {
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
        stagnationClock = System.currentTimeMillis();
        return conn;
    }

    /**
     * 返还 Hbase 连接，从繁忙列表中移除，并加入空闲队列，同时消除其对应的停滞时间。
     * @param connection 期望返还的连接
     * @since 1.0.0
     */
    public synchronized static void flybackConnection(Connection connection){
        int index = busyConnectionsList.indexOf(connection);
        busyConnectionsList.remove(connection);
        busyConnectionsTimes.remove(index);         // 消除停滞时间
        spareConnectionsQueue.offer(connection);
        stagnationClock = System.currentTimeMillis();
    }

    /**
     * 创建一个 Hbase 连接
     * @return 一个连接
     * @since 1.0.0
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
     * @throws HbaseGoSelfException 一个属于 HbaseGo 的 Hbase 连接异常
     * @since 1.0.0
     */
    static void connectHbase() throws HbaseGoSelfException {
        if (ip == null){
            throw new HbaseGoSelfException("IP cannot be null...");
        }
        if(port == null){
            throw new HbaseGoSelfException("Port cannot be null...");
        }
        conf.set("hbase.zookeeper.quorum", ip);
        conf.set("hbase.zookeeper.property.clientPort", port);
        for(int i = 0; i < initHbaseConnectionsNum; i++){
            spareConnectionsQueue.offer(getHbaseConnection());
        }
        // 通知日志记录器
        HbaseGoLoggerProxy.recordMsg(null, HbaseGoLogType.INFO, "Initialize the HbaseGo connection pool successfully");
        // Hbase Connection 自管理连接线程
        new Thread(() -> {
            HbaseGoStatusCollector.setSelfManagingThreadState(HbaseGoSoulState.RUNNING);
            while (!stopSelfManagerFlag) {
                try {                                                           // 本 try 的目的是为了保证线程不会因未知错误中断
                    int length = busyConnectionsTimes.size();
                    for (int i = 0; i < length; i++) {
                        if (busyConnectionsTimes.get(i).value >= hbaseConnectionOutTime) {
                            flybackConnection(busyConnectionsList.get(i));      // 停滞时间达到上限自回收
                            i--;                                                // 必须使 i - 1
                            length--;                                           // 必须使 length - 1
                            HbaseGoLoggerProxy.recordMsg(null, HbaseGoLogType.INFO, "Auto recycle a out-time Hbase connection.");
                        } else {
                            busyConnectionsTimes.get(i).value++;                // 停滞时间未达到上限，使其停滞时间增加
                        }
                    }
                    if (busyConnectionsList.size() == 0 && spareConnectionsQueue.size() != 0 && System.currentTimeMillis() - stagnationClock >= hbaseConnectionOutTime * 1000 * 4){
                        Connection conn;
                        while ((conn = spareConnectionsQueue.poll()) != null){
                            conn.close();
                        }
                    }
                    // 报告状态收集器
                    HbaseGoStatusCollector.setSpareConnectionsNum(spareConnectionsQueue.size());
                    HbaseGoStatusCollector.setBusyConnectionsNum(busyConnectionsList.size());
                    HbaseGoStatusCollector.setBusyConnectionsTimesListSize(busyConnectionsTimes.size());
                    Thread.sleep(1000);                                   // 线程休眠
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            HbaseGoStatusCollector.setSelfManagingThreadState(HbaseGoSoulState.STOPPED);
        }).start();
        HbaseGoLoggerProxy.recordMsg(null, HbaseGoLogType.INFO, "Self-managed thread started.");
    }

    /**
     * 一个用于管理停滞时间的类
     *
     * @author Thonnn 2017-11-26
     * @version 1.0.0
     * @since 1.0.0
     */
    private static class MyInt{
        int value = 0;

        @Override
        public String toString() {
            return value+"";
        }
    }
}
