package com.alibaba.otter.clave.progress.select;

import com.alibaba.otter.clave.common.lifecycle.ClaveLifeCycle;

/**
 * 增量数据获取
 * 
 * @author jianghang 2013-3-28 下午09:20:16
 * @version 1.0.0
 */
public interface ClaveSelector<T> extends ClaveLifeCycle {

    /**
     * 获取一批待处理的数据
     */
    public Message<T> selector() throws InterruptedException;

    /**
     * 反馈一批数据处理失败，需要下次重新被处理
     */
    public void rollback(Long batchId);

    /**
     * 反馈所有的batch数据需要被重新处理
     */
    public void rollback();

    /**
     * 反馈一批数据处理完成
     */
    public void ack(Long batchId);

}
