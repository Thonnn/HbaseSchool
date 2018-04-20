package com.thonnn.hbasego.interfaces;

import com.thonnn.hbasego.HbaseGo;
import com.thonnn.hbasego.dao.HbaseGoDAO;
import com.thonnn.hbasego.dao.HbaseGoVersionBean;
import com.thonnn.hbasego.logger.HbaseGoLogType;
import com.thonnn.hbasego.logger.HbaseGoLoggerProxy;

import java.util.List;

/**
 * 接口名字面意思
 * @author Thonnn 2017-11-26
 * @version 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
 * @since 1.0.0 初次定义
 * @see HbaseGoDAO
 */
public interface IHbaseGoDAOSearch {
    /**
     * 按照 bean 中的数据分页式搜索，如果 rowkey 不为空则按照 rowkey 查询，在进行rowkey查询之后会进行数据碰撞校验， 如果数据无法碰撞，即使根据 rowkey 查询到了数据也会返回空列表；
     * 你可以采取将 bean 中的除了 rowkey 映射的字段以外的其他字段都设置为空(空的，或者是 null)的方法取消这种碰撞机制；
     * 当 rowkey 为空时则按照其他条件查询
     * @param bean 相当于存储了搜索条件的 bean
     * @param <T> 泛型上界为 IHbaseGoBean，其表示必须是实现了IHbaseGoBean 接口的类！
     * @param page_size 分页大小
     * @param page_index 分页当前页
     * @return 一个存储了搜索结果的 List，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     * @since 1.0.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends IHbaseGoBean> List<T> search(T bean, int page_size, int page_index){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>search(T, int, int) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(bean.getClass().getName()).tableNmae);
        return null;
    }

    /**
     * 不分页查询，如果 rowkey 不为空则按照 rowkey 查询，在进行rowkey查询之后会进行数据配装校验， 如果数据无法碰撞，即使根据 rowkey 查询到了数据也会返回空列表；
     * 你可以采取将 bean 中的除了 rowkey 映射的字段以外的其他字段都设置为空(空的，或者是 null)的方法取消这种碰撞机制；
     * 当 rowkey 为空时则按照其他条件查询
     * @param bean 相当于存储了搜索条件的 bean;
     * @param <T> 泛型上界为 IHbaseGoBean，其表示必须是实现了IHbaseGoBean 接口的类！
     * @return 一个存储了搜索结果的 List，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     * @since 1.0.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends IHbaseGoBean> List<T> search(T bean){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>search(T) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(bean.getClass().getName()).tableNmae);
        return null;
    }

    /**
     * 按照 bean 中的数据分页式搜索，如果 rowkey 不为空则按照 rowkey 查询，在进行rowkey查询之后，【不再进行数据碰撞】；
     * 当 rowkey 为空时则按照其他条件查询；
     * 需要指定查询的版本数量，因此返回的数据结构为 HbaseGoVersionBean
     * @param bean 相当于存储了搜索条件的 bean
     * @param page_size 分页大小
     * @param page_index 分页当前页
     * @param maxVersion 设定查询的版本数量，注意，如果数据库中有三个版本从老到新为1，2，3，当指定检索两个版本时，返回的是第2，3 两个版本的数据；
     *                   其取值必须大于等于-1，特殊的，当取值为“-1”时表示查询 Hbase 存储的所有版本
     * @param <T> 泛型上界为 IHbaseGoBean，其表示必须是实现了IHbaseGoBean 接口的类！
     * @return 一个存储了搜索结果的 List，其内部是 HbaseGoVersionBean 类型的，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     * @since 1.1.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends IHbaseGoBean> List<HbaseGoVersionBean<T>> search(T bean, int page_size, int page_index, int maxVersion){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>search(T, int, int, int) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(bean.getClass().getName()).tableNmae);
        return null;
    }

    /**
     * 按照 bean 中的数据搜索，如果 rowkey 不为空则按照 rowkey 查询，在进行rowkey查询之后，【不再进行数据碰撞】；
     * 当 rowkey 为空时则按照其他条件查询；
     * 需要指定查询的版本数量，因此返回的数据结构为 HbaseGoVersionBean
     * @param bean 相当于存储了搜索条件的 bean;
     * @param maxVersion 设定查询的版本数量，注意，如果数据库中有三个版本从老到新为1，2，3，当指定检索两个版本时，返回的是第2，3 两个版本的数据；
     *                   其取值必须大于等于-1，特殊的，当取值为“-1”时表示查询 Hbase 存储的所有版本
     * @param <T> 泛型上界为 IHbaseGoBean，其表示必须是实现了IHbaseGoBean 接口的类！
     * @return 一个存储了搜索结果的 List，其内部是 HbaseGoVersionBean 类型的，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     * @since 1.1.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends IHbaseGoBean> List<HbaseGoVersionBean<T>> search(T bean, int maxVersion){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>search(T, int) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(bean.getClass().getName()).tableNmae);
        return null;
    }

    /**
     * 根据 RowKey 的范围进行搜索，Hbase 的 RowKey 使用了 ASCII 排序
     * @param beanClass 继承自IHbaseGoBean 的Bean的类，本工程使用这个参数进行索引找到其对应的Hbase表关系，并利用反射获取这个类的实例。
     * @param minRowKey RowKey 搜索下限
     * @param maxRowKey RowKey 搜索上限
     * @param page_size 分页大小
     * @param page_index 分页当前页
     * @param <T> 泛型参数，上界为IHbaseGoBean
     * @return 一个存储了搜索结果的 List，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     * @since 1.1.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends  IHbaseGoBean> List<T> searchByRowKeyRange(Class<T> beanClass, Object minRowKey, Object maxRowKey, int page_size, int page_index){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>searchByRowKeyRange(Class<T>, Object, Object, int, int) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(beanClass.getName()).tableNmae);
        return null;
    }

    /**
     * 根据 RowKey 的范围进行搜索，Hbase 的 RowKey 使用了 ASCII 排序
     * @param beanClass 继承自IHbaseGoBean 的Bean的类，本工程使用这个参数进行索引找到其对应的Hbase表关系，并利用反射获取这个类的实例。
     * @param minRowKey RowKey 搜索下限
     * @param maxRowKey RowKey 搜索上限
     * @param <T> 泛型参数，上界为IHbaseGoBean
     * @return 一个存储了搜索结果的 List，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     * @since 1.1.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends  IHbaseGoBean> List<T> searchByRowKeyRange(Class<T> beanClass, Object minRowKey, Object maxRowKey){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>searchByRowKeyRange(Class<T>, Object, Object) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(beanClass.getName()).tableNmae);
        return null;
    }

    /**
     * 根据 RowKey 的范围进行搜索，Hbase 的 RowKey 使用了 ASCII 排序；
     * 需要指定查询的版本数量，因此返回的数据结构为 HbaseGoVersionBean
     * @param beanClass 继承自IHbaseGoBean 的Bean的类，本工程使用这个参数进行索引找到其对应的Hbase表关系，并利用反射获取这个类的实例。
     * @param minRowKey RowKey 搜索下限
     * @param maxRowKey RowKey 搜索上限
     * @param page_size 分页大小
     * @param page_index 分页当前页
     * @param maxVersion 设定查询的版本数量，注意，如果数据库中有三个版本从老到新为1，2，3，当指定检索两个版本时，返回的是第2，3 两个版本的数据；
     *                   其取值必须大于等于-1，特殊的，当取值为“-1”时表示查询 Hbase 存储的所有版本
     * @param <T> 泛型参数，上界为IHbaseGoBean
     * @return 一个存储了搜索结果的 List，其内部是 HbaseGoVersionBean 类型的，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     * @since 1.1.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends  IHbaseGoBean> List<HbaseGoVersionBean<T>> searchByRowKeyRange(Class<T> beanClass, Object minRowKey, Object maxRowKey, int page_size, int page_index, int maxVersion){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>searchByRowKeyRange(Class<T>, Object, Object, int, int, int) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(beanClass.getName()).tableNmae);
        return null;
    }

    /**
     * 根据 RowKey 的范围进行搜索，Hbase 的 RowKey 使用了 ASCII 排序；
     * 需要指定查询的版本数量，因此返回的数据结构为 HbaseGoVersionBean
     * @param beanClass 继承自IHbaseGoBean 的Bean的类，本工程使用这个参数进行索引找到其对应的Hbase表关系，并利用反射获取这个类的实例。
     * @param minRowKey RowKey 搜索下限
     * @param maxRowKey RowKey 搜索上限
     * @param maxVersion 设定查询的版本数量，注意，如果数据库中有三个版本从老到新为1，2，3，当指定检索两个版本时，返回的是第2，3 两个版本的数据；
     *                   其取值必须大于等于-1，特殊的，当取值为“-1”时表示查询 Hbase 存储的所有版本
     * @param <T> 泛型参数，上界为IHbaseGoBean
     * @return 一个存储了搜索结果的 List，其内部是 HbaseGoVersionBean 类型的，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     * @since 1.1.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends  IHbaseGoBean> List<HbaseGoVersionBean<T>> searchByRowKeyRange(Class<T> beanClass, Object minRowKey, Object maxRowKey, int maxVersion){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>searchByRowKeyRange(Class<T>, Object, Object, int) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(beanClass.getName()).tableNmae);
        return null;
    }

    /**
     * 搜索结果数量统计，建议在正式搜索之前执行本方法进行搜索结果数量的预估
     * @param bean 相当于存储了搜索条件的 bean
     * @return 满足搜索条件的结果的数量
     * @since 1.0.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default long searchCount(IHbaseGoBean bean){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.searchCount(IHbaseGoBean) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(bean.getClass().getName()).tableNmae);
        return -1L;
    }

    /**
     * 通过RowKey范围查询，搜索结果数量统计，建议在正式搜索之前执行本方法进行搜索结果数量的预估
     * @param beanClass 继承自IHbaseGoBean 的Bean的类，本工程使用这个参数进行索引找到其对应的Hbase表关系，并利用反射获取这个类的实例。
     * @param minRowKey RowKey 搜索下限
     * @param maxRowKey RowKey 搜索上限
     * @param <T> 泛型参数，上界为IHbaseGoBean
     * @return 满足搜索条件的结果的数量
     * @since 1.1.0 初次定义
     * @since <br> 1.2.0 在所有接口中都增加了默认实现，用以对日志记录器的支持，如果你尝试实现此接口尝试定义自己的 DAO 请特别注意其业务逻辑，以避免日志记录器重复记录。
     */
    default <T extends  IHbaseGoBean> long searchRowKeyRangeCount(Class<T> beanClass, Object minRowKey, Object maxRowKey){
        HbaseGoLoggerProxy.recordMsg(this, HbaseGoLogType.INFO, "HbaseGoDao.<T>searchRowKeyRangeCount(Class<T>, Object, Object) to HbaseTable: " + HbaseGo.tableBeanHashMap.get(beanClass.getName()).tableNmae);
        return -1L;
    }
}
