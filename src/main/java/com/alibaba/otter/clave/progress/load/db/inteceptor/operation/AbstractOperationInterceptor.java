package com.alibaba.otter.clave.progress.load.db.inteceptor.operation;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.alibaba.otter.clave.ClaveConfig;
import com.alibaba.otter.clave.common.dialect.DbDialect;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.load.db.DbLoadContext;
import com.alibaba.otter.clave.progress.load.interceptor.AbstractLoadInterceptor;

/**
 * @author jianghang 2011-10-31 下午02:24:28
 * @version 1.0.0
 */
public abstract class AbstractOperationInterceptor extends AbstractLoadInterceptor<DbLoadContext, EventData> {

    protected int                  serverId            = -1;
    protected final Logger         logger              = LoggerFactory.getLogger(getClass());
    protected static final int     GLOBAL_THREAD_COUNT = 1000;
    protected static final int     INNER_THREAD_COUNT  = 300;
    protected static final String  checkDataSql        = "SELECT COUNT(*) FROM {0} WHERE id BETWEEN 0 AND {1}";
    protected static final String  deleteDataSql       = "DELETE FROM {0}";

    protected String               updateSql;
    protected String               clearSql            = "UPDATE {0} SET {1} = 0 WHERE id = ? and {1} = ?";
    protected int                  innerIdCount        = INNER_THREAD_COUNT;
    protected int                  globalIdCount       = GLOBAL_THREAD_COUNT;
    protected Set<JdbcTemplate>    tableCheckStatus    = Collections.synchronizedSet(new HashSet<JdbcTemplate>());
    protected AtomicInteger        THREAD_COUNTER      = new AtomicInteger(0);
    protected ThreadLocal<Integer> threadLocal         = new ThreadLocal<Integer>();

    protected AbstractOperationInterceptor(String updateSql){
        this.updateSql = updateSql;
    }

    private void init(final JdbcTemplate jdbcTemplate, final String markTableName, final String markTableColumn) {
        int count = jdbcTemplate.queryForInt(MessageFormat.format(checkDataSql, markTableName, GLOBAL_THREAD_COUNT - 1));
        if (count != GLOBAL_THREAD_COUNT) {
            if (logger.isInfoEnabled()) {
                logger.info("Interceptor: init " + markTableName + "'s data.");
            }
            TransactionTemplate transactionTemplate = new TransactionTemplate();
            transactionTemplate.setTransactionManager(new DataSourceTransactionManager(jdbcTemplate.getDataSource()));
            transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NOT_SUPPORTED);// 注意这里强制使用非事务，保证多线程的可见性
            transactionTemplate.execute(new TransactionCallback() {

                public Object doInTransaction(TransactionStatus status) {
                    jdbcTemplate.execute(MessageFormat.format(deleteDataSql, markTableName));
                    String batchSql = MessageFormat.format(updateSql, new Object[] { markTableName, markTableColumn });
                    jdbcTemplate.batchUpdate(batchSql, new BatchPreparedStatementSetter() {

                        public void setValues(PreparedStatement ps, int idx) throws SQLException {
                            ps.setInt(1, idx);
                            ps.setInt(2, 0);
                        }

                        public int getBatchSize() {
                            return GLOBAL_THREAD_COUNT;
                        }
                    });
                    return null;
                }
            });

            if (logger.isInfoEnabled()) {
                logger.info("Interceptor: Init EROSA Client Data: " + updateSql);
            }
        }

    }

    public void transactionBegin(DbLoadContext context, List<EventData> currentDatas, DbDialect dialect) {
        threadLocal.remove();// 进入之前先清理
        int threadId = currentId();
        updateMark(context, dialect, threadId, updateSql);
        threadLocal.set(threadId);
    }

    public void transactionEnd(DbLoadContext context, List<EventData> currentDatas, DbDialect dialect) {
        Integer threadId = threadLocal.get();
        updateMark(context, dialect, threadId, clearSql);
        threadLocal.remove();
    }

    /**
     * 更新一下事务标记
     */
    private void updateMark(DbLoadContext context, DbDialect dialect, int threadId, String sql) {
        String markTableName = ClaveConfig.SYSTEM_SCHEMA + "." + ClaveConfig.SYSTEM_MARK_TABLE;
        String markTableColumn = ClaveConfig.SYSTEM_TABLE_MARK_ID_COLUMN;
        synchronized (dialect.getJdbcTemplate()) {
            if (tableCheckStatus.contains(dialect.getJdbcTemplate()) == false) {
                init(dialect.getJdbcTemplate(), markTableName, markTableColumn);
                tableCheckStatus.add(dialect.getJdbcTemplate());
            }
        }

        int affectedCount = dialect.getJdbcTemplate().update(
                                                             MessageFormat.format(sql, new Object[] { markTableName,
                                                                     markTableColumn }),
                                                             new Object[] { threadId, serverId });
        if (affectedCount <= 0) {
            logger.warn("## update {} failed by [{}]", markTableName, threadId);
        }
    }

    private int currentId() {
        synchronized (this) {
            if (THREAD_COUNTER.get() == INNER_THREAD_COUNT) {
                THREAD_COUNTER.set(0);
            }

            return THREAD_COUNTER.incrementAndGet();
        }
    }

    // ========================= setter / getter ========================

    public void setInnerIdCount(int innerIdCount) {
        this.innerIdCount = innerIdCount;
    }

    public void setGlobalIdCount(int globalIdCount) {
        this.globalIdCount = globalIdCount;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

}
