package com.alibaba.otter.clave.progress.load.db;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.otter.clave.common.dialect.DbDialect;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.load.AbstractLoadContext;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

/**
 * 数据库load处理上下文
 * 
 * @author jianghang 2011-11-1 上午11:22:45
 * @version 4.0.0
 */
public class DbLoadContext extends AbstractLoadContext<EventData> {

    private static final long                serialVersionUID = -4851977997313104740L;

    private DbDialect                        dbDialect;
    private Map<List<String>, DbLoadCounter> counters;

    public DbLoadContext(){
        super();

        counters = new MapMaker().makeComputingMap(new Function<List<String>, DbLoadCounter>() {

            public DbLoadCounter apply(List<String> names) {
                return new DbLoadCounter(names.get(0), names.get(1));
            }
        });
    }

    public Map<List<String>, DbLoadCounter> getCounters() {
        return counters;
    }

    public void setCounters(Map<List<String>, DbLoadCounter> counters) {
        this.counters = counters;
    }

    public DbDialect getDbDialect() {
        return dbDialect;
    }

    public void setDbDialect(DbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public static class DbLoadCounter {

        private String     schema;
        private String     table;
        private AtomicLong rowSize     = new AtomicLong(0);
        private AtomicLong deleteCount = new AtomicLong(0);
        private AtomicLong updateCount = new AtomicLong(0);
        private AtomicLong insertCount = new AtomicLong(0);

        public DbLoadCounter(String schema, String table){
            this.schema = schema;
            this.table = table;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public AtomicLong getDeleteCount() {
            return deleteCount;
        }

        public void setDeleteCount(AtomicLong deleteCount) {
            this.deleteCount = deleteCount;
        }

        public AtomicLong getUpdateCount() {
            return updateCount;
        }

        public void setUpdateCount(AtomicLong updateCount) {
            this.updateCount = updateCount;
        }

        public AtomicLong getInsertCount() {
            return insertCount;
        }

        public void setInsertCount(AtomicLong insertCount) {
            this.insertCount = insertCount;
        }

        public AtomicLong getRowSize() {
            return rowSize;
        }

        public void setRowSize(AtomicLong rowSize) {
            this.rowSize = rowSize;
        }

    }
}
