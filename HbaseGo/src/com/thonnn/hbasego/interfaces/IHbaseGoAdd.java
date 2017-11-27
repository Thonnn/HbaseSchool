package com.thonnn.hbasego.interfaces;

import java.util.List;

/**
 * 接口名字面意思
 * @author Thonnn 2017-11-26
 */
public interface IHbaseGoAdd {
    boolean add(IHbaseGoBean bean);
    boolean add(List<IHbaseGoBean> beanList);
}
