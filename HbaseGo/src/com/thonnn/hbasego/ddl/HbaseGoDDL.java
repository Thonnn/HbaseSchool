package com.thonnn.hbasego.ddl;

import com.thonnn.hbasego.HbaseGo;
import com.thonnn.hbasego.exceptions.HbaseGoDDLException;
import com.thonnn.hbasego.interfaces.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * HbaseGo 对 Hbase 表结构操作的基类，实现了 IHbaseGoDDLCreate, IHbaseGoDDLAlter, IHbaseGoDDLDelete, IHbaseGoDDLWisdom 四个接口；<br>
 * 本类不再支持继承，但你如果需要，可以通过实现上述接口的方式创建属于自己的 DDL，但是在上述接口中的默认实现中，从 v1.2.0 版本开始增加了对日志记录器的支持，因此请特别注意其实现逻辑。<br>
 * 事实上，我并不推荐你这么做。
 * @author Thonnn 2018-04-19
 * @version 1.2.0
 * @since 1.2.0
 */
public final class HbaseGoDDL implements IHbaseGoDDLCreate, IHbaseGoDDLAlter, IHbaseGoDDLDelete, IHbaseGoDDLWisdom {
    private boolean safely = true;                                         // 安全地获取连接？默认 true

    /**
     * 默认的构造
     * @since 1.2.0
     */
    public HbaseGoDDL(){}

    /**
     * 主动配置构造
     * @param safely    是否安全地获取连接，安全获取是获取一个不会冲突的的连接，但当连接数达到上限时会出现 HbaseGoSelfException ；<br>
     *               如果此值为 false，当连接超过超过配置的最大上限时会不安全地从 busyConnectionsList 的 0 位置获取一个连接；<br>
     *               默认为 true。
     * @since 1.0.0
     */
    public HbaseGoDDL(boolean safely){
        this.safely = safely;
    }

    /**
     * 组装 family 属性
     * @param family HbaseGoFamily 列簇对象
     * @return 组装结果
     */
    private HColumnDescriptor assembleHColumnDescriptor(HbaseGoTableFamily family){
        HColumnDescriptor descriptor = new HColumnDescriptor(family.NAME);
        descriptor.setBloomFilterType(family.BLOOMFILTER);
        if(family.MIN_VERSIONS <= 0){
            descriptor.setMaxVersions(family.MAX_VERSIONS);
        }else {
            descriptor.setVersions(family.MIN_VERSIONS, family.MAX_VERSIONS);
        }
        descriptor.setInMemory(family.IN_MEMORY);
        descriptor.setKeepDeletedCells(family.KEEP_DELETED_CELLS);
        descriptor.setDataBlockEncoding(family.DATA_BLOCK_ENCODING);
        descriptor.setTimeToLive(family.TTL);
        descriptor.setCompressionType(family.COMPRESSION);
        descriptor.setBlockCacheEnabled(family.BLOCKCACHE);
        descriptor.setBlocksize(family.BLOCKSIZE);
        descriptor.setScope(family.REPLICATION_SCOPE);
        return descriptor;
    }

    @Override
    public boolean createTable(HbaseGoTable hbaseGoTable) {
        IHbaseGoDDLCreate.super.createTable(hbaseGoTable);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        Admin admin = null;
        try{
            admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(hbaseGoTable.getTableName());
            if(admin.tableExists(tableName)){
                throw new HbaseGoDDLException("Table '" + hbaseGoTable.getTableName() + "' already exist in Hbase when createTable.");
            }else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                if (hbaseGoTable.getFamiliesList() == null || hbaseGoTable.getFamiliesList().size() <= 0){
                    throw new HbaseGoDDLException("HbaseGoTable has nothing family when createTable.");
                }
                for (HbaseGoTableFamily family : hbaseGoTable.getFamiliesList()){
                    hTableDescriptor.addFamily(assembleHColumnDescriptor(family));
                }
                admin.createTable(hTableDescriptor);
                rsl = true;
            }
        }catch (IOException | HbaseGoDDLException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            HbaseGo.flybackConnection(conn);
        }
        return rsl;
    }

    @Override
    public boolean addFamily(String tableName, HbaseGoTableFamily family) {
        IHbaseGoDDLAlter.super.addFamily(tableName, family);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        Admin admin = null;
        try{
            admin = conn.getAdmin();
            TableName atableName = TableName.valueOf(tableName);
            if(!admin.tableExists(atableName)){
                throw new HbaseGoDDLException("Table '" + tableName + "' does not exist in Hbase when addFamily.");
            }else {
                if(admin.getTableDescriptor(atableName).getFamily(Bytes.toBytes(family.NAME)) != null){
                    throw new HbaseGoDDLException("Family '" + family.NAME + "' already exist in Hbase when addFamily.");
                }
                HColumnDescriptor descriptor = assembleHColumnDescriptor(family);
                admin.disableTable(atableName);
                admin.addColumn(atableName, descriptor);
                admin.enableTable(atableName);
                rsl = true;
            }
        }catch (IOException | HbaseGoDDLException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            HbaseGo.flybackConnection(conn);
        }
        return rsl;
    }

    @Override
    public boolean deleteFamily(String tableName, String familyName) {
        IHbaseGoDDLAlter.super.deleteFamily(tableName, familyName);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        Admin admin = null;
        try{
            admin = conn.getAdmin();
            TableName atableName = TableName.valueOf(tableName);
            if(!admin.tableExists(atableName)){
                throw new HbaseGoDDLException("Table '" + tableName + "' does not exist in Hbase when deleteFamily.");
            }else {
                if(admin.getTableDescriptor(atableName).getFamily(Bytes.toBytes(familyName)) == null){
                    throw new HbaseGoDDLException("Family '" + familyName + "' does not exist in Hbase when deleteFamily.");
                }
                admin.disableTable(atableName);
                admin.deleteColumn(atableName, Bytes.toBytes(familyName));
                admin.enableTable(atableName);
                rsl = true;
            }
        }catch (IOException | HbaseGoDDLException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            HbaseGo.flybackConnection(conn);
        }
        return rsl;
    }

    @Override
    public boolean alterFamily(String tableName, HbaseGoTableFamily family) {
        IHbaseGoDDLAlter.super.alterFamily(tableName, family);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        Admin admin = null;
        try{
            admin = conn.getAdmin();
            TableName atableName = TableName.valueOf(tableName);
            if(!admin.tableExists(atableName)){
                throw new HbaseGoDDLException("Table '" + tableName + "' does not exist in Hbase when alterFamily.");
            }else {
                if(admin.getTableDescriptor(atableName).getFamily(Bytes.toBytes(family.NAME)) == null){
                    throw new HbaseGoDDLException("Family '" + family.NAME + "' does not exist in Hbase when alterFamily.");
                }
                HColumnDescriptor descriptor = assembleHColumnDescriptor(family);
                admin.disableTable(atableName);
                admin.modifyColumn(atableName, descriptor);
                admin.enableTable(atableName);
                rsl = true;
            }
        }catch (IOException | HbaseGoDDLException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            HbaseGo.flybackConnection(conn);
        }
        return rsl;
    }

    @Override
    public boolean alterTable(HbaseGoTable hbaseGoTable) {
        IHbaseGoDDLAlter.super.alterTable(hbaseGoTable);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        Admin admin = null;
        try{
            admin = conn.getAdmin();
            TableName tableName = TableName.valueOf(hbaseGoTable.getTableName());
            if(!admin.tableExists(tableName)){
                throw new HbaseGoDDLException("Table '" + hbaseGoTable.getTableName() + "' does not exist in Hbase when alterTable.");
            }else {
                if (hbaseGoTable.getFamiliesList() == null || hbaseGoTable.getFamiliesList().size() <= 0){
                    throw new HbaseGoDDLException("HbaseGoTable has nothing family when alterTable.");
                }
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for (HbaseGoTableFamily family : hbaseGoTable.getFamiliesList()){
                    hTableDescriptor.addFamily(assembleHColumnDescriptor(family));
                }
                admin.disableTable(tableName);
                admin.modifyTable(tableName, hTableDescriptor);
                admin.enableTable(tableName);
                rsl = true;
            }
        }catch (IOException | HbaseGoDDLException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            HbaseGo.flybackConnection(conn);
        }
        return rsl;
    }

    @Override
    public boolean deleteTable(String tableName) {
        IHbaseGoDDLDelete.super.deleteTable(tableName);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        Admin admin = null;
        try{
            admin = conn.getAdmin();
            TableName atableName = TableName.valueOf(tableName);
            if(!admin.tableExists(atableName)){
                throw new HbaseGoDDLException("Table '" + tableName + "' does not exist in Hbase when deleteTable.");
            }else {
                admin.disableTable(atableName);
                admin.deleteTable(atableName);
                rsl = true;
            }
        }catch (IOException | HbaseGoDDLException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            HbaseGo.flybackConnection(conn);
        }
        return rsl;
    }

    @Override
    public boolean truncateTable(String tableName, boolean preserveSplits) {
        IHbaseGoDDLWisdom.super.truncateTable(tableName, preserveSplits);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        Admin admin = null;
        try{
            admin = conn.getAdmin();
            TableName atableName = TableName.valueOf(tableName);
            if(!admin.tableExists(atableName)){
                throw new HbaseGoDDLException("Table '" + tableName + "' does not exist in Hbase when truncateTable.");
            }else {
                admin.disableTable(atableName);
                admin.truncateTable(atableName, preserveSplits);
                rsl = true;
            }
        }catch (IOException | HbaseGoDDLException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            HbaseGo.flybackConnection(conn);
        }
        return rsl;
    }

    @Override
    public boolean truncateTable(String tableName){
        return truncateTable(tableName, true);
    }

    @Override
    public boolean existTable(String tableName) {
        IHbaseGoDDLWisdom.super.existTable(tableName);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        Admin admin = null;
        try{
            admin = conn.getAdmin();
            TableName atableName = TableName.valueOf(tableName);
            rsl = admin.tableExists(atableName);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            HbaseGo.flybackConnection(conn);
        }
        return rsl;
    }
}
