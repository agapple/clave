package com.alibaba.otter.clave.progress.load;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * 数据处理上下文
 * 
 * @author jianghang 2013-3-28 下午10:36:29
 * @version 1.0.0
 */
public abstract class AbstractLoadContext<T> implements LoadContext, Serializable {

    private static final long serialVersionUID = -2052280419851872736L;
    protected List<T>         prepareDatas;                            // 准备处理的数据
    protected List<T>         processedDatas;                          // 已处理完成的数据
    protected List<T>         failedDatas;

    public AbstractLoadContext(){
        this.prepareDatas = Collections.synchronizedList(new LinkedList<T>());
        this.processedDatas = Collections.synchronizedList(new LinkedList<T>());
        this.failedDatas = Collections.synchronizedList(new LinkedList<T>());
    }

    public List<T> getPrepareDatas() {
        return prepareDatas;
    }

    public void setPrepareDatas(List<T> prepareDatas) {
        this.prepareDatas = prepareDatas;
    }

    public void addProcessData(T processData) {
        this.processedDatas.add(processData);
    }

    public List<T> getProcessedDatas() {
        return processedDatas;
    }

    public void setProcessedDatas(List<T> processedDatas) {
        this.processedDatas = processedDatas;
    }

    public List<T> getFailedDatas() {
        return failedDatas;
    }

    public void addFailedData(T failedData) {
        this.failedDatas.add(failedData);
    }

    public void setFailedDatas(List<T> failedDatas) {
        this.failedDatas = failedDatas;
    }

}
