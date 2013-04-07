package com.alibaba.otter.clave.progress.load.interceptor;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.otter.clave.common.dialect.DbDialect;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.load.LoadContext;

public class ChainLoadInterceptor extends AbstractLoadInterceptor<LoadContext, EventData> {

    private List<LoadInterceptor> interceptors = new ArrayList<LoadInterceptor>();

    public void prepare(LoadContext context) {
        if (interceptors == null) {
            return;
        }

        for (LoadInterceptor interceptor : interceptors) {
            interceptor.prepare(context);
        }
    }

    public boolean before(LoadContext context, EventData currentData) {
        if (interceptors == null) {
            return false;
        }

        boolean result = false;
        for (LoadInterceptor interceptor : interceptors) {
            result |= interceptor.before(context, currentData);
            if (result) {// 出现一个true就退出
                return result;
            }
        }
        return result;
    }

    public void transactionBegin(LoadContext context, List<EventData> currentDatas, DbDialect dialect) {
        if (interceptors == null) {
            return;
        }

        for (LoadInterceptor interceptor : interceptors) {
            interceptor.transactionBegin(context, currentDatas, dialect);
        }
    }

    public void transactionEnd(LoadContext context, List<EventData> currentDatas, DbDialect dialect) {
        if (interceptors == null) {
            return;
        }

        for (LoadInterceptor interceptor : interceptors) {
            interceptor.transactionEnd(context, currentDatas, dialect);
        }
    }

    public void after(LoadContext context, EventData currentData) {
        if (interceptors == null) {
            return;
        }

        for (LoadInterceptor interceptor : interceptors) {
            interceptor.after(context, currentData);
        }
    }

    public void commit(LoadContext context) {
        if (interceptors == null) {
            return;
        }

        for (LoadInterceptor interceptor : interceptors) {
            interceptor.commit(context);
        }
    }

    public void error(LoadContext context) {
        if (interceptors == null) {
            return;
        }

        for (LoadInterceptor interceptor : interceptors) {
            interceptor.error(context);
        }
    }

    public void setInterceptors(List<LoadInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

}
