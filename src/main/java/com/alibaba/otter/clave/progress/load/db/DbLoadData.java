package com.alibaba.otter.clave.progress.load.db;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.model.EventType;

/**
 * 将同一个weight下的EventData进行数据归类,按表和insert/update/delete类型进行分类
 * 
 * <pre>
 * 归类用途：对insert语句进行batch优化
 * 1. mysql索引的限制，需要避免insert并发执行
 * </pre>
 * 
 * @author jianghang 2011-11-9 下午04:28:35
 * @version 1.0.0
 */
public class DbLoadData {

    private List<TableLoadData> tableDatas = new ArrayList<TableLoadData>();

    public DbLoadData(){
        // nothing
    }

    public DbLoadData(List<EventData> datas){
        for (EventData data : datas) {
            merge(data);
        }
    }

    public void merge(EventData data) {
        TableLoadData tableData = findTableData(data.getSchemaName(), data.getTableName());

        EventType type = data.getEventType();
        if (type.isInsert()) {
            tableData.getInsertDatas().add(data);
        } else if (type.isUpdate()) {
            tableData.getUpadateDatas().add(data);
        } else if (type.isDelete()) {
            tableData.getDeleteDatas().add(data);
        }
    }

    public List<TableLoadData> getTables() {
        return tableDatas;
    }

    private synchronized TableLoadData findTableData(String schema, String table) {
        for (TableLoadData tableData : tableDatas) {
            if (StringUtils.equalsIgnoreCase(schema, tableData.getSchema())
                && StringUtils.equalsIgnoreCase(table, tableData.getTable())) {
                return tableData;
            }
        }

        TableLoadData data = new TableLoadData(schema, table);
        tableDatas.add(data);
        return data;
    }

    /**
     * 按table进行分类
     */
    public static class TableLoadData {

        private String          schema;
        private String          table;
        private List<EventData> insertDatas  = new ArrayList<EventData>();
        private List<EventData> upadateDatas = new ArrayList<EventData>();
        private List<EventData> deleteDatas  = new ArrayList<EventData>();

        public TableLoadData(String schema, String table){
            this.schema = schema;
            this.table = table;
        }

        public List<EventData> getInsertDatas() {
            return insertDatas;
        }

        public void setInsertDatas(List<EventData> insertDatas) {
            this.insertDatas = insertDatas;
        }

        public List<EventData> getUpadateDatas() {
            return upadateDatas;
        }

        public void setUpadateDatas(List<EventData> upadateDatas) {
            this.upadateDatas = upadateDatas;
        }

        public List<EventData> getDeleteDatas() {
            return deleteDatas;
        }

        public void setDeleteDatas(List<EventData> deleteDatas) {
            this.deleteDatas = deleteDatas;
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

    }
}
