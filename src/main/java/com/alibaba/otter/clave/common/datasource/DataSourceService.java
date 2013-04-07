package com.alibaba.otter.clave.common.datasource;

/**
 * 数据介质源抽象
 * 
 * @author jianghang 2013-3-28 下午10:57:15
 * @version 1.0.0
 */
public interface DataSourceService {

    /**
     * 返回操作数据源的句柄
     * 
     * @param <T>
     * @param dataMediaId
     * @return
     */
    <T> T getDataSource(DataMediaSource dataMediaSource);

    /**
     * 释放当前数据源.
     * 
     * @param pipeline
     */
    void destroy(DataMediaSource dataMediaSource);

    /**
     * 释放所有资源
     */
    void destroy();

}
