package com.alibaba.otter.clave.progress.transform;

import com.alibaba.otter.clave.common.lifecycle.ClaveLifeCycle;

public interface ClaveTransform<T> extends ClaveLifeCycle {

    public T transform(T data);
}
