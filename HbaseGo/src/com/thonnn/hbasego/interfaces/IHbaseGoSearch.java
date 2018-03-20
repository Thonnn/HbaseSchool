package com.thonnn.hbasego.interfaces;

import java.util.List;

/**
 * 接口名字面意思
 * @author Thonnn 2017-11-26
 */
public interface IHbaseGoSearch {
    <T> List<T> search(T bean, int page_size, int page_index);

    /**
     * 不分页查询，如果 rowkey 不为空则按照 rowkey 查询，在进行rowkey查询之后会进行数据配装校验， 如果数据无法碰撞，即使根据 rowkey 查询到了数据也会返回空列表；
     * 你可以采取将 bean 中的除了 rowkey 映射的字段以外的其他字段都设置为空(空的，或者是 null)的方法取消这种碰撞机制；
     * 当 rowkey 为空时则按照其他条件查询
     * @param bean 相当于存储了搜索条件的 bean
     * @return 一个存储了搜索结果的 List，正常来说这个值不会是 null 的，当搜索不到数据时一般会返回一个大小为 0 的 List
     */
    default  <T> List<T> search(T bean){
        return search(bean, 0, 0);
    };
    Long searchCount(IHbaseGoBean bean);
}
