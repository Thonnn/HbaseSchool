package com.thonnn.hbasego.dao;

import com.thonnn.hbasego.exceptions.HBaseGoAlterException;
import com.thonnn.hbasego.exceptions.HbaseGoDeleteException;
import com.thonnn.hbasego.exceptions.HbaseGoSearchException;
import com.thonnn.hbasego.interfaces.*;
import com.thonnn.hbasego.utils.BytesUtil;
import com.thonnn.hbasego.utils.IDUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * HbaseGo 对 Hbase 操作的基类，实现了IHbaseGoAdd, IHbaseGoSearch, IHbaseGoAlter, IHbaseGoDelete 四个接口；
 *
 * @author Thonnn 2017-11-26
 */
public class HbaseGoToTableDAO implements IHbaseGoAdd, IHbaseGoSearch, IHbaseGoAlter, IHbaseGoDelete {
    private BytesUtil bytesUtil = new BytesUtil();                          // 对象 - 字节组转换工具
    private boolean safely = true;                                         // 安全地获取连接？默认 true
    private boolean alterExistCheck = false;                               // 修改时是否进行安全性校验，默认 true
    public HbaseGoToTableDAO(){}                                             // 默认地构造

    /**
     * 主动配置构造
     * @param safely    是否安全地获取连接，安全获取是获取一个不会冲突的的连接，但当连接数达到上限时会出现 OutOfMaxHbaseConnectonsNumException ；
     *               如果此值为 false，当连接超过超过配置的最大上限时会不安全地从 busyConnectionsList 的0位置获取一个连接；
     *               默认为 true。
     * @param alterExistCheck   是否对“修改”操作进行安全性检查，当修改地操作执行于一个不存在地数据时，如果不进行检查，则会在 Hbase 中创建这个数据；
     *                          如果此值为 true，则会进行安全性检查，操作于一个不存在的数据会打印出数据不存在异常，但进行安全性检查会消耗更多地时间、空间资源
     *                          默认的，false
     */
    public HbaseGoToTableDAO(boolean safely, boolean alterExistCheck){
        this.safely = safely;
        this.alterExistCheck = alterExistCheck;
    }

    /**
     * 添加数据，直接传入一个实现了接口 IHaseGoBean 的实例，则会自动根据配置的映射关系添加到 Hbase
     * @param bean  准备添加的 bean
     * @return  是否添加成功
     */
    @Override
    public boolean add(IHbaseGoBean bean) {
        boolean rsl = false;
        try {
            HbaseGoTable hbaseGoTable = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTable.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            if (RowKey == null){                                                    // 校验 RowKey 如果为空则自动从 IDUtil 生成一个 RowKey
                RowKey = IDUtil.getID();
                rowKeyField.set(bean, RowKey);
            }
            Set<String> familyKeySet = hbaseGoTable.familyMap.keySet();
            Put put = new Put(bytesUtil.toBytes(RowKey));
            assemblePut(bean, hbaseGoTable, familyKeySet, put);                     // 组装 Put
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTable.tableNmae));
            table.put(put);
            table.close();
            HbaseGo.flybackConnection(conn);
            rsl = true;
        } catch (NoSuchFieldException | IllegalAccessException | IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 使用整表添加的方式将数据添加到映射关系对应 Hbase 中，支持实现了 IHBaseGoBean 的多个不同类 bean 对象同时插入；
     * 即：可以使用一个 List 同时存储实现于接口 IHBaseGoBean 的多个不同类 Temp1、Temp2、Temp3 …… 的多个不同对象同时数据添加。
     * @param beanList 欲操作的 bean 的列表
     * @return 是否添加成功
     */
    @Override
    public boolean add(List<IHbaseGoBean> beanList) {
        boolean rsl = false;
        try{
            HashMap<String, List<Put>> putMap = new HashMap<>();                    // 组织 Put
            for(IHbaseGoBean bean : beanList){
                HbaseGoTable hbaseGoTable = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
                Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTable.rowkey);
                rowKeyField.setAccessible(true);
                Object RowKey = rowKeyField.get(bean);
                if (RowKey == null){
                    RowKey = IDUtil.getID();
                    rowKeyField.set(bean, RowKey);
                }
                Set<String> familyKeySet = hbaseGoTable.familyMap.keySet();
                Put put = new Put(bytesUtil.toBytes(RowKey));
                assemblePut(bean, hbaseGoTable, familyKeySet, put);                 // 组装 Put
                putMap.computeIfAbsent(hbaseGoTable.tableNmae, k -> new ArrayList<>());
                putMap.get(hbaseGoTable.tableNmae).add(put);
            }
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Set<String> putKeySet = putMap.keySet();
            for(String putKey : putKeySet){                                         // 添加到对应的表中
                Table table = conn.getTable(TableName.valueOf(putKey));
                table.put(putMap.get(putKey));
                table.close();
            }
            HbaseGo.flybackConnection(conn);
            rsl = true;
        } catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 组装 Put
     * @param bean  传入的 bean 对象
     * @param hbaseGoTable 表映射关系对象
     * @param familyKeySet 列簇键集合
     * @param put   目标 Put
     * @throws NoSuchFieldException 找不到文件
     * @throws IllegalAccessException 无法访问字段
     */
    private void assemblePut(IHbaseGoBean bean, HbaseGoTable hbaseGoTable, Set<String> familyKeySet, Put put) throws NoSuchFieldException, IllegalAccessException {
        for (String familyKey : familyKeySet){
            Field familyField = bean.getClass().getDeclaredField(hbaseGoTable.familyMap.get(familyKey));
            familyField.setAccessible(true);
            HashMap srcMap = (HashMap<?, ?>)familyField.get(bean);
            if(srcMap == null || srcMap.size() == 0){
                continue;
            }
            Set cellKeySet = srcMap.keySet();
            for(Object cellKey : cellKeySet){
                put.addColumn(familyKey.getBytes(), bytesUtil.toBytes(cellKey), bytesUtil.toBytes(srcMap.get(cellKey)));
            }
        }
    }

    /**
     * 按照 bean 中的数据分页式搜索，如果 rowkey 不为空则按照 rowkey 查询，在进行rowkey查询之后会进行数据配装校验， 如果数据无法碰撞，即使根据 rowkey 查询到了数据也会返回空列表；
     * 你可以采取将 bean 中的除了 rowkey 映射的字段以外的其他字段都设置为空(空的，或者是 null)的方法取消这种碰撞机制；
     * 当 rowkey 为空时则按照其他条件查询
     * @param bean 相当于存储了搜索条件的 bean
     * @param page_size 分页大小
     * @param page_index 分页当前页
     * @return 一个存储了搜索结果的 List，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     */
    @Override
    public <T> List<T> search(T bean, int page_size, int page_index) {
        List<T> rsl = new ArrayList<>();
        try{
            if(page_size < 0){
                throw new HbaseGoSearchException("Page_Size mast more than 0.");
            }
            if (page_index < 0){
                throw new HbaseGoSearchException("Page_Index mast more than 0.");
            }
            HbaseGoTable hbaseGoTable = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTable.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTable.tableNmae));
            Set<String> familyKeySet = hbaseGoTable.familyMap.keySet();
            if (RowKey != null){                                                    // 检查 rowkey，不为空则按照 rowkey 查询
                Get get = new Get(bytesUtil.toBytes(RowKey));
                Result r = table.get(get);
                IHbaseGoBean currentBean = ((IHbaseGoBean) bean).cloneThis();        // 自定义的深度克隆方法，克隆一个bean
                for (String familyKey : familyKeySet){
                    Field familyField = currentBean.getClass().getDeclaredField(hbaseGoTable.familyMap.get(familyKey));
                    familyField.setAccessible(true);
                    NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(familyKey.getBytes());
                    NavigableSet<byte[]> cellKeySet =  familyMap.navigableKeySet();
                    HashMap<Object, Object> beanMap = new HashMap<>();
                    HashMap<?, ?> srcMap = (HashMap<?, ?>) familyField.get(currentBean);
                    if(srcMap != null && srcMap.size() != 0){
                        for(byte[] cellKey : cellKeySet){
                            Object keyobj =  bytesUtil.toObject(cellKey);
                            Object valobj = bytesUtil.toObject(familyMap.get(cellKey));
                            Object srcobj = srcMap.get(keyobj);
                            if (srcobj != null && !srcobj.equals(valobj)){          // 数据碰撞
                                table.close();
                                HbaseGo.flybackConnection(conn);
                                return new ArrayList<>();
                            }
                            beanMap.put(keyobj, valobj);
                        }
                    }else {                                                         // 数据不碰撞
                        for(byte[] cellKey : cellKeySet){
                            Object keyobj =  bytesUtil.toObject(cellKey);
                            Object valobj = bytesUtil.toObject(familyMap.get(cellKey));
                            beanMap.put(keyobj, valobj);
                        }
                    }
                    familyField.set(currentBean, beanMap);
                }
                rsl.add((T) currentBean);
            }else {
                List<Filter> filters = new ArrayList<>();                           // rowkey 为空则使用过滤器的方式查询
                assembleFilter(((IHbaseGoBean) bean), hbaseGoTable, familyKeySet, filters);          // 组装 Filter
                Scan scan = new Scan();
                scan.setFilter(new FilterList(filters));
                ResultScanner rscan = table.getScanner(scan);
                int count = 0;
                for(Result r : rscan){
                    if(page_size == 0 || count >= page_size * page_index){
                        IHbaseGoBean currentBean = ((IHbaseGoBean) bean).cloneThis();
                        rowKeyField.set(currentBean, bytesUtil.toObject(r.getRow()));
                        for (String familyKey : familyKeySet){
                            Field familyField = currentBean.getClass().getDeclaredField(hbaseGoTable.familyMap.get(familyKey));
                            familyField.setAccessible(true);
                            NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(familyKey.getBytes());
                            NavigableSet<byte[]> cellKeySet =  familyMap.navigableKeySet();
                            Object tempMap = familyField.get(currentBean);
                            HashMap<Object, Object> beanMap = tempMap != null ? (HashMap) tempMap : new HashMap<>();
                            if(tempMap == null){
                                familyField.set(currentBean, beanMap);
                            }
                            for(byte[] cellKey : cellKeySet){
                                beanMap.put(bytesUtil.toObject(cellKey), bytesUtil.toObject(familyMap.get(cellKey)));
                            }
                            if(page_size != 0){
                                count++;
                                if(count >= page_size * page_index + page_size){
                                    break;
                                }
                            }
                        }
                        rsl.add((T) currentBean);
                    }else {
                        count++;
                    }
                }
            }
            table.close();
            HbaseGo.flybackConnection(conn);
        } catch (IllegalAccessException | NoSuchFieldException | IOException | HbaseGoSearchException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 搜索结果数量统计，建议在正式搜索之前执行本方法进行搜索结果数量的预估
     * @param bean 相当于存储了搜索条件的 bean
     * @return 满足搜索条件的结果的数量
     */
    @Override
    public Long searchCount(IHbaseGoBean bean) {
        Long rsl = 0L;
        try{
            HbaseGoTable hbaseGoTable = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTable.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTable.tableNmae));
            Set<String> familyKeySet = hbaseGoTable.familyMap.keySet();
            if (RowKey != null){
                Get get = new Get(bytesUtil.toBytes(RowKey));
                Result r = table.get(get);
                for (String familyKey : familyKeySet){
                    Field familyField = bean.getClass().getDeclaredField(hbaseGoTable.familyMap.get(familyKey));
                    familyField.setAccessible(true);
                    NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(familyKey.getBytes());
                    NavigableSet<byte[]> cellKeySet =  familyMap.navigableKeySet();
                    HashMap<?, ?> srcMap = (HashMap<?, ?>) familyField.get(bean);
                    if(srcMap != null){
                        for(byte[] cellKey : cellKeySet){
                            Object keyobj =  bytesUtil.toObject(cellKey);
                            Object valobj = bytesUtil.toObject(familyMap.get(cellKey));
                            Object srcobj = srcMap.get(keyobj);
                            if (srcobj != null && !srcobj.equals(valobj)){          // 数据碰撞
                                table.close();
                                HbaseGo.flybackConnection(conn);
                                return 0L;
                            }
                        }
                    }
                }
                rsl = 1L;
            }else {
                List<Filter> filters = new ArrayList<>();
                assembleFilter(bean, hbaseGoTable, familyKeySet, filters);          // 组装 Filter
                Scan scan = new Scan();
                scan.setFilter(new FilterList(filters));
                ResultScanner rscan = table.getScanner(scan);
                for(@SuppressWarnings("unused") Result r : rscan){
                    rsl++;
                }
            }
            table.close();
            HbaseGo.flybackConnection(conn);
        } catch (IllegalAccessException | NoSuchFieldException | IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 组装 Filter
     * @param bean 相当于存储了搜索条件的 bean
     * @param hbaseGoTable 操作的表的映射关系
     * @param familyKeySet  列簇集合
     * @param filters   欲操作的 Filter
     */
    private void assembleFilter(IHbaseGoBean bean, HbaseGoTable hbaseGoTable, Set<String> familyKeySet, List<Filter> filters){
        for (String familyKey : familyKeySet){
            try{
                Field familyField = bean.getClass().getDeclaredField(hbaseGoTable.familyMap.get(familyKey));
                familyField.setAccessible(true);
                HashMap<?, ?> srcMap = (HashMap<?, ?>) familyField.get(bean);
                if (srcMap == null || srcMap.size() == 0){
                    continue;
                }
                Set srckeySet = srcMap.keySet();
                for(Object srckey : srckeySet){
                    filters.add(new SingleColumnValueExcludeFilter(familyKey.getBytes(), bytesUtil.toBytes(srckey), CompareFilter.CompareOp.EQUAL, bytesUtil.toBytes(srcMap.get(srckey))));
                }
            } catch (IllegalAccessException | NoSuchFieldException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 单个修改数据，请注意在初始化本类时的 alterExistCheck 参数
     * @param bean 欲操作的对象
     * @return  返回是否修改成功
     */
    @Override
    public boolean alter(IHbaseGoBean bean) {
        boolean rsl = false;
        try{
            HbaseGoTable hbaseGoTable = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTable.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            if (RowKey == null){
                throw new HBaseGoAlterException("RowKey cannot be null");
            }
            if(alterExistCheck){
                Connection conn = HbaseGo.getHbaseConnection(safely);
                Get get = new Get(bytesUtil.toBytes(RowKey));
                Table table = conn.getTable(TableName.valueOf(hbaseGoTable.tableNmae));
                Result r = table.get(get);
                if(r.size() <= 0){
                    throw new HBaseGoAlterException("No data found of RowKey "+ RowKey);
                }
                table.close();
                HbaseGo.flybackConnection(conn);
            }
            rsl = add(bean);
        } catch (IllegalAccessException | NoSuchFieldException | IOException | HBaseGoAlterException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 批量修改数据，请注意在初始化本类时的 alterExistCheck 参数；
     * 支持实现了 IHBaseGoBean 的多个不同类 bean 对象同时修改；
     * 即：可以使用一个 List 同时存储实现于接口 IHBaseGoBean 的多个不同类 Temp1、Temp2、Temp3 …… 的多个不同对象同时数据修改。
     * @param beanList 欲修改的 bean 的列表，
     * @return 是否修改成功
     */
    @Override
    public boolean alter(List<IHbaseGoBean> beanList) {
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        try{
            for(IHbaseGoBean bean : beanList){
                HbaseGoTable hbaseGoTable = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
                Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTable.rowkey);
                rowKeyField.setAccessible(true);
                Object RowKey = rowKeyField.get(bean);
                if (RowKey == null){
                    throw new HBaseGoAlterException("RowKey cannot be null");
                }
                if(alterExistCheck){
                    Get get = new Get(bytesUtil.toBytes(RowKey));
                    Table table = conn.getTable(TableName.valueOf(hbaseGoTable.tableNmae));
                    Result r = table.get(get);
                    if(r.size() <= 0){
                        throw new HBaseGoAlterException("No data found of RowKey "+ RowKey);
                    }
                    table.close();
                }
            }
            HbaseGo.flybackConnection(conn);
            rsl = add(beanList);
        } catch (IllegalAccessException | IOException | NoSuchFieldException | HBaseGoAlterException e) {
            HbaseGo.flybackConnection(conn);
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 单条删除，需要在 bean 中的 rowkey 映射字段存储了rowkey 的值
     * @param bean 欲操作的 bean
     * @return 是否删除成功
     */
    @Override
    public boolean delete(IHbaseGoBean bean) {
        boolean rsl = false;
        try{
            HbaseGoTable hbaseGoTable = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTable.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            if (RowKey == null){
                throw new HbaseGoDeleteException("RowKey cannot be null");
            }
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTable.tableNmae));
            Delete delete = new Delete(bytesUtil.toBytes(RowKey));
            table.delete(delete);
            table.close();
            HbaseGo.flybackConnection(conn);
            rsl = true;
        } catch (IllegalAccessException | NoSuchFieldException | HbaseGoDeleteException | IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 批量删除，需要在 bean 中的 rowkey 映射字段存储了rowkey 的值；
     * 支持实现了 IHBaseGoBean 的多个不同类 bean 数据同时删除；
     * 即：可以使用一个 List 同时存储实现于接口 IHBaseGoBean 的多个不同类 Temp1、Temp2、Temp3 …… 的多个不同对象同时数据删除。
     * @param beanList 欲操作的 bean 的列表
     * @return 是否删除成功
     */
    @Override
    public boolean delete(List<IHbaseGoBean> beanList) {
        boolean rsl = false;
        try {
            HashMap<String, List<Delete>> deleteMap = new HashMap<>();
            for(IHbaseGoBean bean : beanList){
                HbaseGoTable hbaseGoTable = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
                Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTable.rowkey);
                rowKeyField.setAccessible(true);
                Object RowKey = rowKeyField.get(bean);
                if (RowKey == null){
                    throw new HbaseGoDeleteException("RowKey cannot be null");
                }
                Delete delete = new Delete(bytesUtil.toBytes(RowKey));
                deleteMap.computeIfAbsent(hbaseGoTable.tableNmae, k -> new ArrayList<>());
                deleteMap.get(hbaseGoTable.tableNmae).add(delete);
            }
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Set<String> deleteKeySet = deleteMap.keySet();
            for(String deleteKey : deleteKeySet){
                Table table = conn.getTable(TableName.valueOf(deleteKey));
                table.delete(deleteMap.get(deleteKey));
                table.close();
            }
            HbaseGo.flybackConnection(conn);
            rsl = true;
        } catch (HbaseGoDeleteException | IllegalAccessException | NoSuchFieldException | IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }
}
