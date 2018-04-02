package com.thonnn.hbasego.interfaces;

import java.util.List;

/**
 * 接口名字面意思
 * @author Thonnn 2017-11-26
 * @version 1.0.0
 * @since 1.0.0
 * @see com.thonnn.hbasego.dao.HbaseGoToTableDAO
 */
public interface IHbaseGoAlter {
    boolean alter(IHbaseGoBean bean);
    boolean alter(List<IHbaseGoBean> beanList);
}
