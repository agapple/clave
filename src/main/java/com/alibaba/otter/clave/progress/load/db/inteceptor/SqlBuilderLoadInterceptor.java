package com.alibaba.otter.clave.progress.load.db.inteceptor;

import java.util.List;

import org.springframework.util.CollectionUtils;

import com.alibaba.otter.clave.common.dialect.DbDialect;
import com.alibaba.otter.clave.common.dialect.SqlTemplate;
import com.alibaba.otter.clave.model.EventColumn;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.model.EventType;
import com.alibaba.otter.clave.progress.load.db.DbLoadContext;
import com.alibaba.otter.clave.progress.load.interceptor.AbstractLoadInterceptor;

/**
 * 计算下最新的sql语句
 * 
 * @author jianghang 2011-12-26 下午12:09:20
 * @version 4.0.0
 */
public class SqlBuilderLoadInterceptor extends AbstractLoadInterceptor<DbLoadContext, EventData> {

    private boolean rowMode = false;

    public boolean before(DbLoadContext context, EventData currentData) {
        // 初步构建sql
        DbDialect dbDialect = context.getDbDialect();
        SqlTemplate sqlTemplate = dbDialect.getSqlTemplate();
        EventType type = currentData.getEventType();
        String sql = null;

        // 注意insert/update语句对应的字段数序都是将主键排在后面
        if (type.isInsert()) {
            if (CollectionUtils.isEmpty(currentData.getColumns())) { // 如果表为全主键，直接进行insert sql
                sql = sqlTemplate.getInsertSql(currentData.getSchemaName(), currentData.getTableName(),
                                               buildColumnNames(currentData.getKeys()),
                                               buildColumnNames(currentData.getColumns()));
            } else {
                sql = sqlTemplate.getMergeSql(currentData.getSchemaName(), currentData.getTableName(),
                                              buildColumnNames(currentData.getKeys()),
                                              buildColumnNames(currentData.getColumns()), new String[] {});
            }
        } else if (type.isUpdate()) {
            // String[] keyColumns = buildColumnNames(currentData.getKeys());
            // String[] otherColumns = buildColumnNames(currentData.getUpdatedColumns());
            // boolean existOldKeys = false;
            // for (String key : keyColumns) {
            // // 找一下otherColumns是否有主键，存在就代表有主键变更
            // if (ArrayUtils.contains(otherColumns, key)) {
            // existOldKeys = true;
            // break;
            // }
            // }

            boolean existOldKeys = !CollectionUtils.isEmpty(currentData.getOldKeys());
            String[] keyColumns = null;
            String[] otherColumns = null;
            if (existOldKeys) {
                // 需要考虑主键变更的场景
                // 构造sql如下：update table xxx set pk = newPK where pk = oldPk
                keyColumns = buildColumnNames(currentData.getOldKeys());
                otherColumns = buildColumnNames(currentData.getUpdatedColumns(), currentData.getKeys());
            } else {
                keyColumns = buildColumnNames(currentData.getKeys());
                otherColumns = buildColumnNames(currentData.getUpdatedColumns());
            }

            if (rowMode && !existOldKeys) {// 如果是行记录,并且不存在主键变更，考虑merge sql
                sql = sqlTemplate.getMergeSql(currentData.getSchemaName(), currentData.getTableName(), keyColumns,
                                              otherColumns, new String[] {});
            } else {// 否则进行update sql
                sql = sqlTemplate.getUpdateSql(currentData.getSchemaName(), currentData.getTableName(), keyColumns,
                                               otherColumns);
            }
        } else if (type.isDelete()) {
            sql = sqlTemplate.getDeleteSql(currentData.getSchemaName(), currentData.getTableName(),
                                           buildColumnNames(currentData.getKeys()));
        }
        currentData.setSql(sql);
        return false;
    }

    private String[] buildColumnNames(List<EventColumn> columns) {
        String[] result = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            EventColumn column = columns.get(i);
            result[i] = column.getColumnName();
        }
        return result;
    }

    private String[] buildColumnNames(List<EventColumn> columns1, List<EventColumn> columns2) {
        String[] result = new String[columns1.size() + columns2.size()];
        int i = 0;
        for (i = 0; i < columns1.size(); i++) {
            EventColumn column = columns1.get(i);
            result[i] = column.getColumnName();
        }

        for (; i < columns1.size() + columns2.size(); i++) {
            EventColumn column = columns2.get(i - columns1.size());
            result[i] = column.getColumnName();
        }
        return result;
    }

    public void setRowMode(boolean rowMode) {
        this.rowMode = rowMode;
    }

}
