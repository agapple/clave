package com.alibaba.otter.clave.progress;

import com.alibaba.otter.clave.common.lifecycle.AbstractClaveLifeCycle;
import com.alibaba.otter.clave.progress.extract.ClaveExtractor;
import com.alibaba.otter.clave.progress.load.ClaveLoader;
import com.alibaba.otter.clave.progress.transform.ClaveTransform;

/**
 * ETL调度器
 * 
 * @author jianghang 2013-4-7 下午03:08:10
 * @version 1.0.0
 */
public class ClaveProgress<T> extends AbstractClaveLifeCycle {

    private ClaveExtractor<T> extract;
    private ClaveTransform<T> transform;
    private ClaveLoader<T>    load;

    public void start() {
        if (extract != null && !extract.isStart()) {
            extract.start();
        }

        if (transform != null && !transform.isStart()) {
            transform.start();
        }

        if (load != null && !load.isStart()) {
            load.start();
        }

        super.start();
    }

    public void stop() {
        super.stop();

        if (extract != null && extract.isStart()) {
            extract.stop();
        }

        if (transform != null && transform.isStart()) {
            transform.stop();
        }

        if (load != null && load.isStart()) {
            load.stop();
        }
    }

    /**
     * 处理一批数据，如果处理成功返回true，如果处理失败返回false
     * 
     * @param data
     * @return
     */
    public boolean process(T data) {
        try {
            if (extract != null) {
                extract.extract(data);
            }

            if (transform != null) {
                data = transform.transform(data);
            }

            if (load != null) {
                load.load(data);
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    public void setExtract(ClaveExtractor<T> extract) {
        this.extract = extract;
    }

    public void setTransform(ClaveTransform<T> transform) {
        this.transform = transform;
    }

    public void setLoad(ClaveLoader<T> load) {
        this.load = load;
    }

}
