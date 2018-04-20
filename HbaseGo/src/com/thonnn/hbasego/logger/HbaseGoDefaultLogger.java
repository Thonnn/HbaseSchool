package com.thonnn.hbasego.logger;

import com.thonnn.hbasego.exceptions.HBaseGoLoggerException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 默认日志记录器
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public final class HbaseGoDefaultLogger implements IHbaseGoLogger{
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
    private DateFormat dateFormat_fileName = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss-SSS");
    private int maxCacheSize = 200;
    private String logFileDir = null;
    private File logFile = null;
    private FileWriter logFileWriter = null;
    private LogWriteThread logWriteThread = null;
    private boolean startWriteFlag = false;
    private boolean stopWriteFlag = false;
    private List<String> dataCache = new LinkedList<>();
    private IHbaseGoLogPrinter logPrinter = null;

    /**
     * 设置最大内存缓存区数量大小
     * @param maxCacheSize 内存缓存区数量大小，取值范围应该在 [50, 500]
     * @since 1.2.0
     */
    public synchronized void setMaxCacheSize(int maxCacheSize){
        if(maxCacheSize < 50 || maxCacheSize >=500){
            try {
                throw new HBaseGoLoggerException("MaxCacheSize must be in the range of [50, 500]");
            } catch (HBaseGoLoggerException e) {
                e.printStackTrace();
            }
            return;
        }
        this.maxCacheSize = maxCacheSize;
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.WARN, "Set maxCacheSize = " + maxCacheSize);
    }

    /**
     * 获取内存缓存区数量大小
     * @return 内存缓存区数量大小
     * @since 1.2.0
     */
    public synchronized int getMaxCacheSize(){
        return maxCacheSize;
    }

    /**
     * 设置日志文件所在目录
     * @param logFileDir 目标目录
     * @since 1.2.0
     */
    public synchronized void setLogFileDir(String logFileDir) {
        this.logFileDir = logFileDir;
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.WARN, "Set logFileDir = " + logFileDir);
    }

    /**
     * 获取日志文件所在目录
     * @return 日志文件所在目录
     * @since 1.2.0
     */
    public synchronized String getLogFileDir() {
        return logFileDir;
    }

    /**
     * 设置日志打印机，如果不设定日至打印机，日志不会输出到控制台。<br>
     * 你可以通过创建 HbaseGoDefaultLogPrinter 对象，来创建 工程默认类型的打印机<br>
     * 你可以通过实现 IHbaseGoLogPrinter 接口定义自己的打印规则。
     * @param logPrinter 实现了 IHbaseGoLogPrinter 接口的打印机。
     * @since 1.2.0
     */
    public synchronized void setLogPrinter(IHbaseGoLogPrinter logPrinter){
        this.logPrinter = logPrinter;
        if(logPrinter != null){
            HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.WARN, "LogPrinter is enabled");
        }else {
            HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.WARN, "LogPrinter is disabled");
        }
    }

    /**
     * 获取整个内存缓存区列表，这个操作会清空内存缓存区。
     * @return 内存缓存区列表
     * @since 1.2.0
     */
    public synchronized List<String> getDataCache(){
        List<String> rsl = dataCache;
        dataCache = new LinkedList<>();
        return rsl;
    }

    @Override
    public synchronized void recordMsg(Object currentObject, HbaseGoLogType type, String msg){
        String msgStr = getTime() + "_" + type.toString() + ": " + msg;
        if(dataCache.size() <= maxCacheSize){
            dataCache.add(msgStr);
        }else {
            dataCache.remove(0);
            dataCache.add(msgStr);
        }
        if(logPrinter != null){
            logPrinter.print(currentObject, type, msg);
        }
        writeOut(msgStr);
    }

    /**
     * 开始将日志写入文件指令
     * @since 1.2.0
     */
    public synchronized void startWrite(){
        try {
            if(logFileDir == null) {
                throw new HBaseGoLoggerException("LogFileDir cannot be null, use 'setLogFileDir(String)' to set it first, please.");
            }
            startWriteFlag = true;
            stopWriteFlag = false;
        } catch (HBaseGoLoggerException e) {
            e.printStackTrace();
        }
    }

    /**
     * 停止将日志写入文件指令
     * @since 1.2.0
     */
    public synchronized void stopWrite(){
        try{
            stopWriteFlag = true;
            startWriteFlag = false;
            if(logFileWriter != null){
                logFileWriter.close();
                logFileWriter = null;
            }
            if(logFile != null){
                logFile = null;
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取时间字符串
     * @return 时间字符串
     * @since 1.2.0
     */
    private synchronized String getTime(){
        return "[" + dateFormat.format(new Date()) + "]";
    }

    /**
     * 将日志字符串写入文件
     * @param msg 日志信息字符串
     * @since 1.2.0
     */
    private synchronized void writeOut(String msg){
        if(logWriteThread == null){
            logWriteThread = new LogWriteThread();
            logWriteThread.start();
        }
        if(!startWriteFlag || stopWriteFlag){
            return;
        }
        try {
            logWriteThread.msgQueue.put(msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写入文件线程
     * @version 1.2.0
     * @since 1.2.0
     */
    private class LogWriteThread extends Thread{
        private boolean stopThreadFlag = false;
        private BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(maxCacheSize);

        @Override
        public void run() {
            super.run();
            while (!stopThreadFlag){
                try{
                    if(stopWriteFlag){
                        msgQueue.clear();
                        sleep(100);
                        continue;
                    }
                    if(logFileDir != null){
                        if(logFile == null){
                            File dir = new File(logFileDir);
                            if(!dir.exists() && !dir.mkdirs()){
                                throw new IOException("HbaseGoDefaultLogger can not create log file dir: " + dir.getPath());
                            }
                            logFile = new File(logFileDir, "HbaseGo_" + dateFormat_fileName.format(new Date()) + ".log");
                            if(logFile.exists()){
                                if(!logFile.delete()){
                                    throw new IOException("HbaseGoDefaultLogger can not delete log file: " + logFile.getPath());
                                }
                            }
                            if(!logFile.createNewFile()){
                                throw new IOException("HbaseGoDefaultLogger can not create log file: " + logFile.getPath());
                            }
                        }
                        if(logFileWriter == null){
                            logFileWriter = new FileWriter(logFile);
                        }
                        logFileWriter.write(msgQueue.take() + "\n");
                    }
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
