package com.thonnn.hbasego.interfaces;

import java.util.List;

/**
 * 接口名字面意思
 * @author Thonnn 2017-11-26
 */
public interface IHbaseGoAlter {
    boolean alter(IHbaseGoBean bean);
    boolean alter(List<IHbaseGoBean> beanList);
}
