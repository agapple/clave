package com.alibaba.otter.clave.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.otter.clave.utils.ClaveToStringStyle;

/**
 * 描述一个变更事件
 * 
 * @author jianghang 2013-3-28 下午09:17:18
 * @version 1.0.0
 */
public class EventData implements ObjectData {

    private static final long serialVersionUID = -7071677425383765372L;

    private String            tableName;

    private String            schemaName;

    /**
     * 变更数据的业务类型(I/U/D/C/A/E),与ErosaProtocol中定义的EventType一致.
     */
    private EventType         eventType;

    /**
     * 变更数据的业务时间.
     */
    private long              executeTime;

    /**
     * 变更前的主键值,如果是insert/delete变更前和变更后的主键值是一样的.
     */
    private List<EventColumn> oldKeys          = new ArrayList<EventColumn>();

    /**
     * 变更后的主键值,如果是insert/delete变更前和变更后的主键值是一样的.
     */
    private List<EventColumn> keys             = new ArrayList<EventColumn>();

    private List<EventColumn> columns          = new ArrayList<EventColumn>();

    // ====================== 运行过程中对数据的附加属性 =============================
    /**
     * 预计的size大小，基于binlog event的推算
     */
    private long              size             = 1024;

    /**
     * 当eventType = CREATE/ALTER/ERASE时，就是对应的sql语句，否则无效.
     */
    private String            sql;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(long executeTime) {
        this.executeTime = executeTime;
    }

    public List<EventColumn> getKeys() {
        return keys;
    }

    public void setKeys(List<EventColumn> keys) {
        this.keys = keys;
    }

    public List<EventColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<EventColumn> columns) {
        this.columns = columns;
    }

    public List<EventColumn> getOldKeys() {
        return oldKeys;
    }

    public void setOldKeys(List<EventColumn> oldKeys) {
        this.oldKeys = oldKeys;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    // ======================== helper method =================

    /**
     * 返回所有待变更的字段
     */
    public List<EventColumn> getUpdatedColumns() {
        List<EventColumn> columns = new ArrayList<EventColumn>();
        for (EventColumn column : this.columns) {
            if (column.isUpdate()) {
                columns.add(column);
            }
        }

        return columns;
    }

    public String toString() {
        return ToStringBuilder.reflectionToString(this, ClaveToStringStyle.DEFAULT_STYLE);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((columns == null) ? 0 : columns.hashCode());
        result = prime * result + ((eventType == null) ? 0 : eventType.hashCode());
        result = prime * result + (int) (executeTime ^ (executeTime >>> 32));
        result = prime * result + ((keys == null) ? 0 : keys.hashCode());
        result = prime * result + ((oldKeys == null) ? 0 : oldKeys.hashCode());
        result = prime * result + ((schemaName == null) ? 0 : schemaName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        EventData other = (EventData) obj;
        if (columns == null) {
            if (other.columns != null) return false;
        } else if (!columns.equals(other.columns)) return false;
        if (eventType != other.eventType) return false;
        if (executeTime != other.executeTime) return false;
        if (keys == null) {
            if (other.keys != null) return false;
        } else if (!keys.equals(other.keys)) return false;
        if (oldKeys == null) {
            if (other.oldKeys != null) return false;
        } else if (!oldKeys.equals(other.oldKeys)) return false;
        if (schemaName == null) {
            if (other.schemaName != null) return false;
        } else if (!schemaName.equals(other.schemaName)) return false;
        if (tableName == null) {
            if (other.tableName != null) return false;
        } else if (!tableName.equals(other.tableName)) return false;
        return true;
    }

}
