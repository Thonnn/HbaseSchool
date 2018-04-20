package com.thonnn.hbasego.dao;

import com.thonnn.hbasego.HbaseGo;
import com.thonnn.hbasego.HbaseGoTableMapper;
import com.thonnn.hbasego.exceptions.HBaseGoDAOException;
import com.thonnn.hbasego.interfaces.*;
import com.thonnn.hbasego.utils.BytesUtil;
import com.thonnn.hbasego.utils.IDUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 * HbaseGo 对 Hbase 表中数据操作的基类，实现了 IHbaseGoDAOAdd, IHbaseGoDAOSearch, IHbaseGoDAOAlter, IHbaseGoDAODelete 四个接口；<br>
 * 本类不再支持继承，但你如果需要，可以通过实现上述接口的方式创建属于自己的 DAO，但是在上述接口中的默认实现中，从 v1.2.0 版本开始增加了对日志记录器的支持，因此请特别注意其实现逻辑。<br>
 * 事实上，我并不推荐你这么做。
 * @author Thonnn 2017-11-26
 * @version 1.2.0 调整了部分实现逻辑，增加关键字 final
 * @since 1.0.0
 */
public final class HbaseGoDAO implements IHbaseGoDAOAdd, IHbaseGoDAOSearch, IHbaseGoDAOAlter, IHbaseGoDAODelete {
    private BytesUtil bytesUtil = new BytesUtil();                          // 对象 - 字节组转换工具
    private boolean safely = true;                                         // 安全地获取连接？默认 true
    private boolean alterExistCheck = false;                              // 修改时是否进行安全性校验，默认 true

    /**
     * 默认的构造
     * @since 1.0.0
     */
    public HbaseGoDAO(){}

    /**
     * 主动配置构造
     * @param safely    是否安全地获取连接，安全获取是获取一个不会冲突的的连接，但当连接数达到上限时会出现 HbaseGoSelfException ；<br>
     *               如果此值为 false，当连接超过超过配置的最大上限时会不安全地从 busyConnectionsList 的0位置获取一个连接；<br>
     *               默认为 true。
     * @param alterExistCheck   是否对“修改”操作进行安全性检查，当修改地操作执行于一个不存在地数据时，如果不进行检查，则会在 Hbase 中创建这个数据；<br>
     *                          如果此值为 true，则会进行安全性检查，操作于一个不存在的数据会打印出数据不存在异常，但进行安全性检查会消耗更多地时间、空间资源 <br>
     *                          默认的，false
     * @since 1.0.0
     */
    public HbaseGoDAO(boolean safely, boolean alterExistCheck){
        this.safely = safely;
        this.alterExistCheck = alterExistCheck;
    }

    @Override
    public boolean add(IHbaseGoBean bean) {
        IHbaseGoDAOAdd.super.add(bean);
        boolean rsl = false;
        try {
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());        // 获取表映射关系
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);                  // 从 Bean 中获取RowKey字段
            rowKeyField.setAccessible(true);                                                            // 使私有字段可见（默认情况下，私有字段在反射时是不可见的）
            Object RowKey = rowKeyField.get(bean);                                                      // 获取RowKey字段的值
            if (RowKey == null){                                                    // 校验 RowKey 如果为空则自动从 IDUtil 生成一个 RowKey
                RowKey = IDUtil.getID();
                rowKeyField.set(bean, RowKey);
            }
            Set<String> familyKeySet = hbaseGoTableMapper.familyMap.keySet();             // 获取列簇的键集合
            Put put = new Put(bytesUtil.toBytes(RowKey));                           // Put 是用于向Hbase写入数据的类
            assemblePut(bean, hbaseGoTableMapper, familyKeySet, put);                     // 组装 Put
            Connection conn = HbaseGo.getHbaseConnection(safely);                   // 获取一个连接
            Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae)); // 获取表（连接表）
            table.put(put);                                                          // 向表中写入数据
            table.close();
            HbaseGo.flybackConnection(conn);
            rsl = true;
        } catch (NoSuchFieldException | IllegalAccessException | IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    @Override
    public boolean add(List<IHbaseGoBean> beanList) {
        IHbaseGoDAOAdd.super.add(beanList);
        boolean rsl = false;
        try{
            HashMap<String, List<Put>> putMap = new HashMap<>();                    // 组织 Put
            for(IHbaseGoBean bean : beanList){
                HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
                Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);
                rowKeyField.setAccessible(true);
                Object RowKey = rowKeyField.get(bean);
                if (RowKey == null){
                    RowKey = IDUtil.getID();
                    rowKeyField.set(bean, RowKey);
                }
                Set<String> familyKeySet = hbaseGoTableMapper.familyMap.keySet();
                Put put = new Put(bytesUtil.toBytes(RowKey));
                assemblePut(bean, hbaseGoTableMapper, familyKeySet, put);                 // 组装 Put
                putMap.computeIfAbsent(hbaseGoTableMapper.tableNmae, k -> new ArrayList<>());
                putMap.get(hbaseGoTableMapper.tableNmae).add(put);
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
     * 组装 Put 这是一个由 IDEA 自动简化代码工具生成的方法
     * @param bean  传入的 bean 对象
     * @param hbaseGoTableMapper 表映射关系对象
     * @param familyKeySet 列簇键集合
     * @param put   目标 Put
     * @throws NoSuchFieldException 找不到文件
     * @throws IllegalAccessException 无法访问字段
     * @since 1.0.0
     */
    private void assemblePut(IHbaseGoBean bean, HbaseGoTableMapper hbaseGoTableMapper, Set<String> familyKeySet, Put put) throws NoSuchFieldException, IllegalAccessException {
        for (String familyKey : familyKeySet){
            Field familyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.familyMap.get(familyKey));            // 获取列簇对应Bean中的对象
            familyField.setAccessible(true);

            HashMap srcMap = (HashMap<?, ?>)familyField.get(bean);
            if(srcMap == null || srcMap.size() == 0){
                continue;
            }
            Set cellKeySet = srcMap.keySet();
            for(Object cellKey : cellKeySet){
                put.addColumn(familyKey.getBytes(), bytesUtil.toBytes(cellKey), bytesUtil.toBytes(srcMap.get(cellKey))); // 组装Put
            }
        }
    }

    @Override
    public <T extends IHbaseGoBean> List<T> search(T bean, int page_size, int page_index) {
        IHbaseGoDAOSearch.super.search(bean, page_size, page_index);
        List<T> rsl = new ArrayList<>();
        try{
            if(page_size < 0){
                throw new HBaseGoDAOException("Page_Size must more than 0.");
            }
            if (page_index < 0){
                throw new HBaseGoDAOException("Page_Index must more than 0.");
            }
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
            Set<String> familyKeySet = hbaseGoTableMapper.familyMap.keySet();
            if (RowKey != null){                                                    // 检查 rowkey，不为空则按照 rowkey 查询
                Get get = new Get(bytesUtil.toBytes(RowKey));
                Result r = table.get(get);
                if (r.isEmpty()){
                    return rsl;
                }
                T currentBean = bean.cloneThis();        // 自定义的深度克隆方法，克隆一个bean
                for (String familyKey : familyKeySet){
                    Field familyField = currentBean.getClass().getDeclaredField(hbaseGoTableMapper.familyMap.get(familyKey));
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
                rsl.add(currentBean);
            }else {
                List<Filter> filters = new ArrayList<>();                           // rowkey 为空则使用过滤器的方式查询
                assembleFilter(bean, hbaseGoTableMapper, familyKeySet, filters);          // 组装 Filter
                Scan scan = new Scan();
                scan.setFilter(new FilterList(filters));
                ResultScanner rscan = table.getScanner(scan);
                int count = 0;
                for(Result r : rscan){
                    if(page_size == 0 || count >= page_size * page_index){
                        T currentBean = bean.cloneThis();
                        rowKeyField.set(currentBean, bytesUtil.toObject(r.getRow()));
                        count = assembleSearchResultList(page_size, page_index, hbaseGoTableMapper, count, r, currentBean, familyKeySet);
                        rsl.add(currentBean);
                    }else {
                        count++;
                    }
                }
            }
            table.close();
            HbaseGo.flybackConnection(conn);
        } catch (IllegalAccessException | NoSuchFieldException | IOException | HBaseGoDAOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    @Override
    public <T extends IHbaseGoBean> List<HbaseGoVersionBean<T>> search(T bean, int page_size, int page_index, int maxVersion){
        IHbaseGoDAOSearch.super.search(bean, page_size, page_index, maxVersion);
        List<HbaseGoVersionBean<T>> rsl = new ArrayList<>();
        try{
            if(page_size < 0){
                throw new HBaseGoDAOException("Page_Size must more than 0.");
            }
            if (page_index < 0){
                throw new HBaseGoDAOException("Page_Index must more than 0.");
            }
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
            Set<String> familyKeySet = hbaseGoTableMapper.familyMap.keySet();
            if (RowKey != null){                                                    // 检查 rowkey，不为空则按照 rowkey 查询
                Get get = new Get(bytesUtil.toBytes(RowKey));
                if (maxVersion == -1){
                    get.setMaxVersions();
                }else if (maxVersion >=0){
                    get.setMaxVersions(maxVersion);
                }else throw new HBaseGoDAOException("HbaseGoDao VERSIONS must be more than or equal to -1.");

                Result r = table.get(get);
                if (r.isEmpty()){
                    return rsl;
                }
                List<Cell> cells = r.listCells();
                HashMap<Long, List<Cell>> cellMap = new HashMap<>();
                assembleCells(cells, cellMap);
                HbaseGoVersionBean<T> rslBean = new HbaseGoVersionBean<>();
                for(Long timestamp : cellMap.keySet()){
                    T currentBean = bean.cloneThis();                           // 自定义的深度克隆方法，克隆一个bean
                    HashMap<String, HashMap<Object, Object>> familyMap = new HashMap<>();
                    assembleFamilyMap(hbaseGoTableMapper, currentBean, familyMap);            // 重新拆封组装 Cell 结构
                    for (Cell cell : cellMap.get(timestamp)){
                        familyMap.get(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))
                                .put(bytesUtil.toObject(Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())),
                                        bytesUtil.toObject(Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
                    }
                    rslBean.add(timestamp, currentBean);
                }
                rsl.add(rslBean);
            }else {
                List<Filter> filters = new ArrayList<>();                           // rowkey 为空则使用过滤器的方式查询
                assembleFilter(bean, hbaseGoTableMapper, familyKeySet, filters);          // 组装 Filter
                Scan scan = new Scan();
                scan.setFilter(new FilterList(filters));
                setSearchMaxVersions(maxVersion, scan);
                ResultScanner rscan = table.getScanner(scan);
                int count = 0;
                for(Result r : rscan){
                    if(page_size == 0 || count >= page_size * page_index){
                        List<Cell> cells = r.listCells();
                        HashMap<Long, List<Cell>> cellMap = new HashMap<>();
                        assembleCells(cells, cellMap);
                        HbaseGoVersionBean<T> rslBean = new HbaseGoVersionBean<>();
                        for(Long timestamp : cellMap.keySet()){
                            T currentBean = bean.cloneThis();                           // 自定义的深度克隆方法，克隆一个bean
                            rowKeyField.set(currentBean, bytesUtil.toObject(r.getRow()));
                            HashMap<String, HashMap<Object, Object>> familyMap = new HashMap<>();
                            assembleFamilyMap(hbaseGoTableMapper, currentBean, familyMap);
                            for (Cell cell : cellMap.get(timestamp)){
                                familyMap.get(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))
                                        .put(bytesUtil.toObject(Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())),
                                                bytesUtil.toObject(Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
                            }
                            rslBean.add(timestamp, currentBean);
                        }
                        rsl.add(rslBean);
                    }else {
                        count++;
                    }
                }
            }
            table.close();
            HbaseGo.flybackConnection(conn);
        }catch (HBaseGoDAOException | IllegalAccessException | NoSuchFieldException | IOException e){
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 设置检索的最大版本，此方法是由 IDEA 的代码自动简化工具自动生成的。
     * @param maxVersion 最大版本号；<br>
     *                   其取值必须大于等于-1，特殊的，当取值为“-1”时表示查询 Hbase 存储的所有版本
     * @param scan 扫描器
     * @throws HBaseGoDAOException 设定版本异常
     * @since 1.1.0
     */
    private void setSearchMaxVersions(int maxVersion, Scan scan) throws HBaseGoDAOException {
        if (maxVersion == -1){
            scan.setMaxVersions();
        }else if (maxVersion >=0){
            scan.setMaxVersions(maxVersion);
        }else {
            throw new HBaseGoDAOException("HbaseGoDao VERSIONS must be more than or equal to -1.");
        }
    }

    /**
     * 组装familyMap，即用于在多版本检索过程中，组装各个Bean对应的列簇HashMap，此方法是由 IDEA 代码自动简化工具自动生成的。
     * @param hbaseGoTableMapper HbaseGo 中存放的表映射关系
     * @param currentBean 当前操作的Bean
     * @param familyMap 列簇的HashMap
     * @param <T> 泛型参数，即 实现了 IHbaseGoBean 的类
     * @throws NoSuchFieldException 字段不存在异常，主要发生在反射数据访问的过程中
     * @throws IllegalAccessException 非法访问异常，主要发生在反射数据访问的过程中
     * @since 1.1.0
     */
    private <T extends IHbaseGoBean> void assembleFamilyMap(HbaseGoTableMapper hbaseGoTableMapper, T currentBean, HashMap<String, HashMap<Object, Object>> familyMap) throws NoSuchFieldException, IllegalAccessException {
        for (String familyKey : hbaseGoTableMapper.familyMap.keySet()){
            Field familyField = currentBean.getClass().getDeclaredField(hbaseGoTableMapper.familyMap.get(familyKey));
            familyField.setAccessible(true);
            Object tempMap = familyField.get(currentBean);
            HashMap<Object, Object> beanMap = tempMap != null ? (HashMap) tempMap : new HashMap<>();
            if(tempMap == null){
                familyField.set(currentBean, beanMap);
            }
            familyMap.put(hbaseGoTableMapper.familyMap.get(familyKey), beanMap);
        }
    }

    /**
     * 重新分类组装Cell结构的方法，次方法为 IDEA 重复代码简化工具自动生成的
     * @param cells Hbase 查询结果的 cell
     * @param cellMap 一个用于存储cell分类结果的HashMap
     * @since 1.1.0
     */
    private void assembleCells(List<Cell> cells, HashMap<Long, List<Cell>> cellMap) {
        if(cells != null && !cells.isEmpty()){
            for(Cell cell:cells){
                long timestamp = cell.getTimestamp();
                cellMap.computeIfAbsent(timestamp, k -> new ArrayList<>());
                cellMap.get(timestamp).add(cell);
            }
        }
    }

    @Override
    public <T extends IHbaseGoBean> List<T> searchByRowKeyRange(Class<T> beanClass, Object minRowKey, Object maxRowKey, int page_size, int page_index){
        IHbaseGoDAOSearch.super.searchByRowKeyRange(beanClass, minRowKey, maxRowKey, page_size, page_index);
        List<T> rsl = new ArrayList<>();
        try {
            if(page_size < 0){
                throw new HBaseGoDAOException("Page_Size must more than 0.");
            }
            if (page_index < 0){
                throw new HBaseGoDAOException("Page_Index must more than 0.");
            }
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(beanClass.getName());
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
            List<Filter> filters = new ArrayList<>();
            filters.add(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(bytesUtil.toBytes(minRowKey))));
            filters.add(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(bytesUtil.toBytes(maxRowKey))));
            Scan scan = new Scan();
            scan.setFilter(new FilterList(filters));
            ResultScanner rscan = table.getScanner(scan);
            int count = 0;
            for(Result r : rscan){
                if(page_size == 0 || count >= page_size * page_index){
                T currentBean = beanClass.newInstance();
                Field rowKeyField = beanClass.getDeclaredField(hbaseGoTableMapper.rowkey);
                rowKeyField.setAccessible(true);
                rowKeyField.set(currentBean, bytesUtil.toObject(r.getRow()));
                Set<String> familyKeySet = hbaseGoTableMapper.familyMap.keySet();
                count = assembleSearchResultList(page_size, page_index, hbaseGoTableMapper, count, r, currentBean, familyKeySet);
                rsl.add(currentBean);
                }else {
                    count++;
                }
            }
            table.close();
            HbaseGo.flybackConnection(conn);
        } catch (InstantiationException | IllegalAccessException | HBaseGoDAOException | IOException | NoSuchFieldException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    @Override
    public <T extends  IHbaseGoBean> List<HbaseGoVersionBean<T>> searchByRowKeyRange(Class<T> beanClass, Object minRowKey, Object maxRowKey, int page_size, int page_index, int maxVersion){
        IHbaseGoDAOSearch.super.searchByRowKeyRange(beanClass, minRowKey, maxRowKey, page_size, page_index, maxVersion);
        List<HbaseGoVersionBean<T>> rsl = new ArrayList<>();
        try{
            if(page_size < 0){
                throw new HBaseGoDAOException("Page_Size must more than 0.");
            }
            if (page_index < 0){
                throw new HBaseGoDAOException("Page_Index must more than 0.");
            }
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(beanClass.getName());
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
            List<Filter> filters = new ArrayList<>();
            filters.add(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(bytesUtil.toBytes(minRowKey))));
            filters.add(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(bytesUtil.toBytes(maxRowKey))));
            Scan scan = new Scan();
            scan.setFilter(new FilterList(filters));
            setSearchMaxVersions(maxVersion, scan);
            ResultScanner rscan = table.getScanner(scan);
            int count = 0;
            for(Result r : rscan){
                if(page_size == 0 || count >= page_size * page_index){
                    List<Cell> cells = r.listCells();
                    HashMap<Long, List<Cell>> cellMap = new HashMap<>();
                    assembleCells(cells, cellMap);
                    HbaseGoVersionBean<T> rslBean = new HbaseGoVersionBean<>();
                    for(Long timestamp : cellMap.keySet()){
                        T currentBean = beanClass.newInstance();
                        Field rowKeyField = beanClass.getDeclaredField(hbaseGoTableMapper.rowkey);
                        rowKeyField.setAccessible(true);
                        rowKeyField.set(currentBean, bytesUtil.toObject(r.getRow()));
                        HashMap<String, HashMap<Object, Object>> familyMap = new HashMap<>();
                        assembleFamilyMap(hbaseGoTableMapper, currentBean, familyMap);
                        for (Cell cell : cellMap.get(timestamp)){
                            familyMap.get(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()))
                                    .put(bytesUtil.toObject(Bytes.copy(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())),
                                            bytesUtil.toObject(Bytes.copy(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())));
                        }
                        rslBean.add(timestamp, currentBean);
                    }
                    rsl.add(rslBean);
                }else {
                    count++;
                }
            }
            table.close();
            HbaseGo.flybackConnection(conn);
        }catch (HBaseGoDAOException | IOException | IllegalAccessException | NoSuchFieldException | InstantiationException e){
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 组装搜索结果的List，次方法是由 IDEA 重代码简化工具自动简化生成的
     * @param page_size 分页大小
     * @param page_index 分页当前页
     * @param hbaseGoTableMapper 表映射关系
     * @param count 计数器
     * @param r scan 的结果 Result
     * @param currentBean 当前正在操作的Bean
     * @param familyKeySet 列簇键的集合
     * @param <T> 泛型参数
     * @return 组装结果
     * @throws NoSuchFieldException 反射时的字段不存在异常
     * @throws IllegalAccessException 反射时的非法访问异常
     * @since 1.1.0
     */
    private <T extends IHbaseGoBean> int assembleSearchResultList(int page_size, int page_index, HbaseGoTableMapper hbaseGoTableMapper, int count, Result r, T currentBean, Set<String> familyKeySet) throws NoSuchFieldException, IllegalAccessException {
        for (String familyKey : familyKeySet){
            Field familyField = currentBean.getClass().getDeclaredField(hbaseGoTableMapper.familyMap.get(familyKey));
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
        return count;
    }

    @Override
    public long searchCount(IHbaseGoBean bean) {
        IHbaseGoDAOSearch.super.searchCount(bean);
        long rsl = 0L;
        try{
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
            Set<String> familyKeySet = hbaseGoTableMapper.familyMap.keySet();
            if (RowKey != null){
                Get get = new Get(bytesUtil.toBytes(RowKey));
                Result r = table.get(get);
                if (r.isEmpty()){
                    return rsl;
                }
                for (String familyKey : familyKeySet){
                    Field familyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.familyMap.get(familyKey));
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
                assembleFilter(bean, hbaseGoTableMapper, familyKeySet, filters);          // 组装 Filter
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

    @Override
    public <T extends IHbaseGoBean> List<T> searchByRowKeyRange(Class<T> beanClass, Object minRowKey, Object maxRowKey) {
        return searchByRowKeyRange(beanClass, minRowKey, maxRowKey, 0, 0);
    }

    @Override
    public <T extends IHbaseGoBean> List<HbaseGoVersionBean<T>> searchByRowKeyRange(Class<T> beanClass, Object minRowKey, Object maxRowKey, int maxVersion) {
        return searchByRowKeyRange(beanClass, minRowKey, maxRowKey, 0, 0, maxVersion);
    }

    @Override
    public <T extends IHbaseGoBean> List<HbaseGoVersionBean<T>> search(T bean, int maxVersion) {
        return search(bean, 0, 0, maxVersion);
    }

    @Override
    public <T extends IHbaseGoBean> List<T> search(T bean) {
        return search(bean, 0, 0);
    }

    @Override
    public <T extends  IHbaseGoBean> long searchRowKeyRangeCount(Class<T> beanClass, Object minRowKey, Object maxRowKey){
        IHbaseGoDAOSearch.super.searchRowKeyRangeCount(beanClass, minRowKey, maxRowKey);
        long rsl = 0L;
        try {
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(beanClass.getName());
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
            List<Filter> filters = new ArrayList<>();
            filters.add(new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(bytesUtil.toBytes(minRowKey))));
            filters.add(new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(bytesUtil.toBytes(maxRowKey))));
            Scan scan = new Scan();
            scan.setFilter(new FilterList(filters));
            ResultScanner rscan = table.getScanner(scan);

            for(Result ignored : rscan){
                rsl++;
            }
            table.close();
            HbaseGo.flybackConnection(conn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    /**
     * 组装 Filter 此方法是由IDEA重复代码简化工具自动简化生成的
     * @param bean 相当于存储了搜索条件的 bean
     * @param hbaseGoTableMapper 操作的表的映射关系
     * @param familyKeySet  列簇集合
     * @param filters   欲操作的 Filter
     * @since 1.0.0
     */
    private void assembleFilter(IHbaseGoBean bean, HbaseGoTableMapper hbaseGoTableMapper, Set<String> familyKeySet, List<Filter> filters){
        for (String familyKey : familyKeySet){
            try{
                Field familyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.familyMap.get(familyKey));
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

    @Override
    public boolean alter(IHbaseGoBean bean) {
        IHbaseGoDAOAlter.super.alter(bean);
        boolean rsl = false;
        try{
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            if (RowKey == null){
                throw new HBaseGoDAOException("RowKey cannot be null");
            }
            if(alterExistCheck){
                Connection conn = HbaseGo.getHbaseConnection(safely);
                Get get = new Get(bytesUtil.toBytes(RowKey));
                Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
                Result r = table.get(get);
                if(r.size() <= 0){
                    throw new HBaseGoDAOException("No data found of RowKey "+ RowKey);
                }
                table.close();
                HbaseGo.flybackConnection(conn);
            }
            rsl = add(bean);
        } catch (IllegalAccessException | NoSuchFieldException | IOException | HBaseGoDAOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    @Override
    public boolean alter(List<IHbaseGoBean> beanList) {
        IHbaseGoDAOAlter.super.alter(beanList);
        boolean rsl = false;
        Connection conn = HbaseGo.getHbaseConnection(safely);
        try{
            for(IHbaseGoBean bean : beanList){
                HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
                Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);
                rowKeyField.setAccessible(true);
                Object RowKey = rowKeyField.get(bean);
                if (RowKey == null){
                    throw new HBaseGoDAOException("RowKey cannot be null");
                }
                if(alterExistCheck){
                    Get get = new Get(bytesUtil.toBytes(RowKey));
                    Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
                    Result r = table.get(get);
                    if(r.size() <= 0){
                        throw new HBaseGoDAOException("No data found of RowKey "+ RowKey);
                    }
                    table.close();
                }
            }
            HbaseGo.flybackConnection(conn);
            rsl = add(beanList);
        } catch (IllegalAccessException | IOException | NoSuchFieldException | HBaseGoDAOException e) {
            HbaseGo.flybackConnection(conn);
            e.printStackTrace();
        }
        return rsl;
    }

    @Override
    public boolean delete(IHbaseGoBean bean) {
        IHbaseGoDAODelete.super.delete(bean);
        boolean rsl = false;
        try{
            HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
            Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);
            rowKeyField.setAccessible(true);
            Object RowKey = rowKeyField.get(bean);
            if (RowKey == null){
                throw new HBaseGoDAOException("RowKey cannot be null");
            }
            Connection conn = HbaseGo.getHbaseConnection(safely);
            Table table = conn.getTable(TableName.valueOf(hbaseGoTableMapper.tableNmae));
            Delete delete = new Delete(bytesUtil.toBytes(RowKey));
            table.delete(delete);
            table.close();
            HbaseGo.flybackConnection(conn);
            rsl = true;
        } catch (IllegalAccessException | NoSuchFieldException | HBaseGoDAOException | IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }

    @Override
    public boolean delete(List<IHbaseGoBean> beanList) {
        IHbaseGoDAODelete.super.delete(beanList);
        boolean rsl = false;
        try {
            HashMap<String, List<Delete>> deleteMap = new HashMap<>();
            for(IHbaseGoBean bean : beanList){
                HbaseGoTableMapper hbaseGoTableMapper = HbaseGo.tableBeanHashMap.get(bean.getClass().getName());
                Field rowKeyField = bean.getClass().getDeclaredField(hbaseGoTableMapper.rowkey);
                rowKeyField.setAccessible(true);
                Object RowKey = rowKeyField.get(bean);
                if (RowKey == null){
                    throw new HBaseGoDAOException("RowKey cannot be null");
                }
                Delete delete = new Delete(bytesUtil.toBytes(RowKey));
                deleteMap.computeIfAbsent(hbaseGoTableMapper.tableNmae, k -> new ArrayList<>());
                deleteMap.get(hbaseGoTableMapper.tableNmae).add(delete);
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
        } catch (HBaseGoDAOException | IllegalAccessException | NoSuchFieldException | IOException e) {
            e.printStackTrace();
        }
        return rsl;
    }
}
