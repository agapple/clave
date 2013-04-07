package com.alibaba.otter.clave.progress.load.interceptor;

import java.util.List;

import com.alibaba.otter.clave.common.dialect.DbDialect;

public interface LoadInterceptor<L, D> {

    public void prepare(L context);

    /**
     * 返回值代表是否需要过滤该记录,true即为过滤不处理
     */
    public boolean before(L context, D currentData);

    public void transactionBegin(L context, List<D> currentDatas, DbDialect dialect);

    public void transactionEnd(L context, List<D> currentDatas, DbDialect dialect);

    public void after(L context, D currentData);

    public void commit(L context);

    public void error(L context);
}
