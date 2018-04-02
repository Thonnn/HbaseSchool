package com.thonnn.hbasego.interfaces;

import com.thonnn.hbasego.utils.BytesUtil;

import java.lang.reflect.Field;

/**
 * 可识别的 bean 的必要接口，所有 bean 必须实现的；
 * 接口中的 default 实现，需要 jdk/jre 1.8 以上版本支持。
 *
 * @author Thonnn 2017-11-26
 * @version 1.0.0
 * @since 1.0.0
 */
public interface IHbaseGoBean {
    BytesUtil bytesUtil = new BytesUtil();

    /**
     * 深度克隆方法。
     * @param <T> 泛型上界为 IHbaseGoBean，其表示必须是实现了IHbaseGoBean 接口的类！
     * @return 深度克隆结果，强制类型转换结果为 T 类型
     * @since 1.1.0
     */
    @SuppressWarnings("unchecked")
    default <T extends IHbaseGoBean> T cloneThis(){
        return (T) bytesUtil.toObject(bytesUtil.toBytes(this));
    }
}
