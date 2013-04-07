package com.alibaba.otter.clave.common.lifecycle;

/**
 * @author jianghang 2012-7-12 上午09:39:33
 * @version 1.0.0
 */
public interface ClaveLifeCycle {

    public void start();

    public void stop();

    public boolean isStart();
}
