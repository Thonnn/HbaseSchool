package com.thonnn.hbasego.interfaces;

import com.thonnn.hbasego.utils.BytesUtil;

import java.lang.reflect.Field;

/**
 * 可识别的 bean 的必要接口，所有 bean 必须实现的；
 * 接口中的 default 实现，需要 jdk/jre 1.8 以上版本支持。
 *
 * @author Thonnn 2017-11-26
 */
public interface IHbaseGoBean {
    BytesUtil bytesUtil = new BytesUtil();
    default IHbaseGoBean cloneThis(){
        return (IHbaseGoBean) bytesUtil.toObject(bytesUtil.toBytes(this));
    }
}
