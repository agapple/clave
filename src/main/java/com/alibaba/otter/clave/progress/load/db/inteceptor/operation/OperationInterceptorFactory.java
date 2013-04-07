package com.alibaba.otter.clave.progress.load.db.inteceptor.operation;

import java.util.List;

import com.alibaba.otter.clave.common.dialect.DbDialect;
import com.alibaba.otter.clave.common.dialect.mysql.MysqlDialect;
import com.alibaba.otter.clave.common.dialect.oracle.OracleDialect;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.load.db.DbLoadContext;
import com.alibaba.otter.clave.progress.load.interceptor.AbstractLoadInterceptor;
import com.alibaba.otter.clave.progress.load.interceptor.LoadInterceptor;

/**
 * operator的调用工厂
 * 
 * @author jianghang 2011-10-31 下午03:20:16
 * @version 1.0.0
 */
public class OperationInterceptorFactory extends AbstractLoadInterceptor<DbLoadContext, EventData> {

    private LoadInterceptor[] mysqlInterceptors;
    private LoadInterceptor[] oracleInterceptors;
    private LoadInterceptor[] empty = new LoadInterceptor[0];

    public void transactionBegin(DbLoadContext context, List<EventData> currentDatas, DbDialect dialect) {
        LoadInterceptor[] interceptors = getIntercetptor(context, currentDatas);
        for (LoadInterceptor interceptor : interceptors) {
            interceptor.transactionBegin(context, currentDatas, dialect);
        }
    }

    public void transactionEnd(DbLoadContext context, List<EventData> currentDatas, DbDialect dialect) {
        LoadInterceptor[] interceptors = getIntercetptor(context, currentDatas);
        for (LoadInterceptor interceptor : interceptors) {
            interceptor.transactionEnd(context, currentDatas, dialect);
        }
    }

    private LoadInterceptor[] getIntercetptor(DbLoadContext context, List<EventData> currentData) {
        if (currentData == null || currentData.size() == 0) {
            return empty;
        }

        DbDialect dbDialect = context.getDbDialect();

        if (dbDialect instanceof MysqlDialect) {
            return mysqlInterceptors;
        } else if (dbDialect instanceof OracleDialect) {
            return oracleInterceptors;
        } else {
            return empty;
        }
    }

    // ===================== setter / getter =========================

    public void setMysqlInterceptors(LoadInterceptor[] mysqlInterceptors) {
        this.mysqlInterceptors = mysqlInterceptors;
    }

    public void setOracleInterceptors(LoadInterceptor[] oracleInterceptors) {
        this.oracleInterceptors = oracleInterceptors;
    }

}
