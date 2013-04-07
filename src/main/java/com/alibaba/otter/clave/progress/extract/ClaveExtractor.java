package com.alibaba.otter.clave.progress.extract;

import com.alibaba.otter.clave.common.lifecycle.ClaveLifeCycle;

public interface ClaveExtractor<T> extends ClaveLifeCycle {

    public void extract(T data);
}
