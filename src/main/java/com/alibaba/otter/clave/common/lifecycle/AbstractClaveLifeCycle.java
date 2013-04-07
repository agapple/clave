package com.alibaba.otter.clave.common.lifecycle;

import com.alibaba.otter.clave.exceptions.ClaveException;

/**
 * 基本实现
 * 
 * @author jianghang 2012-7-12 上午10:11:07
 * @version 1.0.0
 */
public abstract class AbstractClaveLifeCycle implements ClaveLifeCycle {

    protected volatile boolean running = false; // 是否处于运行中

    public boolean isStart() {
        return running;
    }

    public void start() {
        if (running) {
            throw new ClaveException(this.getClass().getName() + " has startup , don't repeat start");
        }

        running = true;
    }

    public void stop() {
        if (!running) {
            throw new ClaveException(this.getClass().getName() + " isn't start , please check");
        }

        running = false;
    }

}
