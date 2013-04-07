package com.alibaba.otter.clave.progress.load;

import com.alibaba.otter.clave.common.lifecycle.ClaveLifeCycle;

/**
 * 数据loader接口
 * 
 * @author jianghang 2013-3-28 下午10:28:45
 * @version 1.0.0
 */
public interface ClaveLoader<T> extends ClaveLifeCycle {

    public void load(T data);
}
