package com.alibaba.otter.clave.progress.load.db;

import com.alibaba.otter.clave.common.datasource.db.DbMediaSource;
import com.alibaba.otter.clave.common.dialect.DbDialectFactory;
import com.alibaba.otter.clave.common.lifecycle.AbstractClaveLifeCycle;
import com.alibaba.otter.clave.model.RowBatch;
import com.alibaba.otter.clave.progress.load.ClaveLoader;
import com.alibaba.otter.clave.progress.load.weight.WeightController;

/**
 * 基于数据库load的实现
 * 
 * @author jianghang 2013-4-7 下午03:24:32
 * @version 1.0.0
 */
public class DataBaseLoader extends AbstractClaveLifeCycle implements ClaveLoader<RowBatch> {

    private DbDialectFactory dbDialectFactory;
    private DbMediaSource    dataSource;
    private DbLoadAction     action;

    public void load(RowBatch rowBatch) {
        WeightController controller = new WeightController(1);
        DbLoadContext context = new DbLoadContext();
        context.setDbDialect(dbDialectFactory.getDbDialect(dataSource));
        action.load(rowBatch, controller, context);
    }

    public void setAction(DbLoadAction action) {
        this.action = action;
    }

    public void setDataSource(DbMediaSource dataSource) {
        this.dataSource = dataSource;
    }

    public void setDbDialectFactory(DbDialectFactory dbDialectFactory) {
        this.dbDialectFactory = dbDialectFactory;
    }

}
