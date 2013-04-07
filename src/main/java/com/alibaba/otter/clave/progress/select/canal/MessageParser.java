package com.alibaba.otter.clave.progress.select.canal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.alibaba.otter.clave.ClaveConfig;
import com.alibaba.otter.clave.exceptions.ClaveException;
import com.alibaba.otter.clave.model.EventColumn;
import com.alibaba.otter.clave.model.EventColumnIndexComparable;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.model.EventType;

/**
 * 数据对象解析
 * 
 * @author jianghang 2013-3-28 下午09:50:33
 * @version 1.0.0
 */
public class MessageParser {

    private static final String RETL_CLIENT_FLAG = "_SYNC";
    private boolean             rowMode          = false;
    private int                 serverId         = -1;

    /**
     * 将对应canal送出来的Entry对象解析为otter使用的内部对象
     * 
     * <pre>
     * 需要处理数据过滤：
     * 1. Transaction Begin/End过滤
     * 2. retl.retl_client/retl.retl_mark 回环标记处理以及后续的回环数据过滤
     * 3. retl.xdual canal心跳表数据过滤
     * </pre>
     */
    public List<EventData> parse(List<Entry> datas) throws ClaveException {
        List<EventData> eventDatas = new ArrayList<EventData>();
        List<Entry> transactionDataBuffer = new ArrayList<Entry>();
        boolean isLoopback = false;
        try {
            for (Entry entry : datas) {
                switch (entry.getEntryType()) {
                    case TRANSACTIONBEGIN:
                        isLoopback = false;
                        break;
                    case ROWDATA:
                        String schemaName = entry.getHeader().getSchemaName();
                        String tableName = entry.getHeader().getTableName();
                        // 判断是否是回环表retl_mark
                        boolean isMarkTable = schemaName.equalsIgnoreCase(ClaveConfig.SYSTEM_SCHEMA)
                                              && tableName.equalsIgnoreCase(ClaveConfig.SYSTEM_MARK_TABLE);
                        if (isMarkTable) {
                            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
                            if (!rowChange.getIsDdl()) {
                                int loopback = checkLoopback(rowChange.getRowDatas(0));
                                isLoopback |= loopback > 0;
                            }
                        }

                        if (!isLoopback && !isMarkTable) {
                            transactionDataBuffer.add(entry);
                        }
                        break;
                    case TRANSACTIONEND:
                        if (!isLoopback) {
                            // 添加数据解析
                            for (Entry bufferEntry : transactionDataBuffer) {
                                List<EventData> parseDatas = internParse(bufferEntry);
                                if (CollectionUtils.isEmpty(parseDatas)) {// 可能为空，针对ddl返回时就为null
                                    continue;
                                }

                                // 初步计算一下事件大小
                                long totalSize = bufferEntry.getHeader().getEventLength();
                                long eachSize = totalSize / parseDatas.size();
                                for (EventData eventData : parseDatas) {
                                    if (eventData == null) {
                                        continue;
                                    }

                                    eventData.setSize(eachSize);// 记录一下大小
                                    eventDatas.add(eventData);
                                }
                            }
                        }

                        isLoopback = false;
                        transactionDataBuffer.clear();
                        break;
                    default:
                        break;
                }
            }

            // 添加最后一次的数据，可能没有TRANSACTIONEND
            if (!isLoopback) {
                // 添加数据解析
                for (Entry bufferEntry : transactionDataBuffer) {
                    List<EventData> parseDatas = internParse(bufferEntry);
                    if (CollectionUtils.isEmpty(parseDatas)) {// 可能为空，针对ddl返回时就为null
                        continue;
                    }

                    // 初步计算一下事件大小
                    long totalSize = bufferEntry.getHeader().getEventLength();
                    long eachSize = totalSize / parseDatas.size();
                    for (EventData eventData : parseDatas) {
                        if (eventData == null) {
                            continue;
                        }

                        eventData.setSize(eachSize);// 记录一下大小
                        eventDatas.add(eventData);
                    }
                }
            }
        } catch (Exception e) {
            throw new ClaveException(e);
        }

        return eventDatas;
    }

    /**
     * <pre>
     * the table def: 
     *              server_info varchar
     *              server_id varchar
     * 每次解析时，每个事务首先获取 retl_mark 下的 server_info 或 server_id 字段变更。
     *  a. 如果存在 server_info 以 '_SYNC'结尾的字符串 ，则忽略本次事务的数据变更；
     *  b. 如果不等于，则执行下面的判断。
     *      i. 如果存在server_id = "xx"，则检查对应的server_id是否为当前同步的serverId，如果是则忽略。
     *      ii. 不存在则不处理
     * </pre>
     */
    private int checkLoopback(RowData rowData) {
        // 检查channel_info字段
        // 首先检查下after记录，从无变有的过程，一般出现在事务头
        Column infokColumn = getColumnIgnoreCase(rowData.getAfterColumnsList(),
                                                 ClaveConfig.SYSTEM_TABLE_MARK_INFO_COLUMN);
        // 匹配对应的channel id
        if (infokColumn != null && infokColumn.getValue().toUpperCase().endsWith(RETL_CLIENT_FLAG)) {
            return 1;
        }

        infokColumn = getColumnIgnoreCase(rowData.getBeforeColumnsList(), ClaveConfig.SYSTEM_TABLE_MARK_INFO_COLUMN);
        if (infokColumn != null && infokColumn.getValue().toUpperCase().endsWith(RETL_CLIENT_FLAG)) {
            return 1;
        }

        // 检查serverId
        Column markColumn = getColumnIgnoreCase(rowData.getAfterColumnsList(), ClaveConfig.SYSTEM_TABLE_MARK_ID_COLUMN);
        if (markColumn != null && markColumn.getValue().equals(serverId)) {
            return 2;
        }

        markColumn = getColumnIgnoreCase(rowData.getBeforeColumnsList(), ClaveConfig.SYSTEM_TABLE_MARK_ID_COLUMN);
        if (markColumn != null && markColumn.getValue().equals(serverId)) {
            return 2;
        }
        return 0;
    }

    private Column getColumnIgnoreCase(List<Column> columns, String columName) {
        for (Column column : columns) {
            if (column.getName().equalsIgnoreCase(columName)) {
                return column;
            }
        }

        return null;
    }

    private List<EventData> internParse(Entry entry) {
        RowChange rowChange = null;
        try {
            rowChange = RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new ClaveException("ERROR ## parser of canal-event has an error , data:" + entry.toString(), e);
        }

        if (rowChange == null) {
            return null;
        }

        List<EventData> eventDatas = new ArrayList<EventData>();
        for (RowData rowData : rowChange.getRowDatasList()) {
            EventData eventData = internParse(entry, rowChange, rowData);
            if (eventData != null) {
                eventDatas.add(eventData);
            }
        }

        return eventDatas;
    }

    /**
     * 解析出从canal中获取的Event事件<br>
     * Oracle:有变更的列值. <br>
     * <i>insert:从afterColumns中获取所有的变更数据<br>
     * <i>delete:从beforeColumns中获取所有的变更数据<br>
     * <i>update:在before中存放所有的主键和变化前的非主键值，在after中存放变化后的主键和非主键值,如果是复合主键，只会存放变化的主键<br>
     * Mysql:可以得到所有变更前和变更后的数据.<br>
     * <i>insert:从afterColumns中获取所有的变更数据<br>
     * <i>delete:从beforeColumns中获取所有的变更数据<br>
     * <i>update:在beforeColumns中存放变更前的所有数据,在afterColumns中存放变更后的所有数据<br>
     */
    private EventData internParse(Entry entry, RowChange rowChange, RowData rowData) {
        EventData eventData = new EventData();
        eventData.setTableName(entry.getHeader().getTableName());
        eventData.setSchemaName(entry.getHeader().getSchemaName());
        eventData.setEventType(EventType.valueOf(rowChange.getEventType().name()));
        eventData.setExecuteTime(entry.getHeader().getExecuteTime());

        EventType eventType = eventData.getEventType();
        // 首先判断是否为系统表
        if (StringUtils.equalsIgnoreCase(ClaveConfig.SYSTEM_SCHEMA, eventData.getSchemaName())) {
            // do noting
            if (eventType.isCreate() || eventType.isAlter() || eventType.isErase()) {
                return null;
            }

            if (StringUtils.equalsIgnoreCase(ClaveConfig.SYSTEM_DUAL_TABLE, eventData.getTableName())) {
                // 心跳表数据直接忽略
                return null;
            }
        }

        List<Column> beforeColumns = rowData.getBeforeColumnsList();
        List<Column> afterColumns = rowData.getAfterColumnsList();

        // 变更后的主键
        Map<String, EventColumn> keyColumns = new LinkedHashMap<String, EventColumn>();
        // 变更前的主键
        Map<String, EventColumn> oldKeyColumns = new LinkedHashMap<String, EventColumn>();
        // 有变化的非主键
        Map<String, EventColumn> notKeyColumns = new LinkedHashMap<String, EventColumn>();
        if (eventType.isInsert()) {
            for (Column column : afterColumns) {
                if (column.getIsKey()) {
                    keyColumns.put(column.getName(), copyEventColumn(column, true));
                } else {
                    // mysql 有效
                    notKeyColumns.put(column.getName(), copyEventColumn(column, true));
                }
            }
        } else if (eventType.isDelete()) {
            for (Column column : beforeColumns) {
                if (column.getIsKey()) {
                    keyColumns.put(column.getName(), copyEventColumn(column, true));
                } else {
                    // mysql 有效
                    notKeyColumns.put(column.getName(), copyEventColumn(column, true));
                }
            }
        } else if (eventType.isUpdate()) {
            // 获取变更前的主键.
            for (Column column : beforeColumns) {
                if (column.getIsKey()) {
                    oldKeyColumns.put(column.getName(), copyEventColumn(column, true));
                } else if (rowMode && entry.getHeader().getSourceType() == CanalEntry.Type.ORACLE) {
                    // 针对行记录同步时，针对oracle记录一下非主键的字段，因为update时针对未变更的字段在aftercolume里没有
                    notKeyColumns.put(column.getName(), copyEventColumn(column, rowMode));
                }
            }
            for (Column column : afterColumns) {
                if (column.getIsKey()) {
                    // 获取变更后的主键
                    keyColumns.put(column.getName(), copyEventColumn(column, true));
                } else if (rowMode || entry.getHeader().getSourceType() == CanalEntry.Type.ORACLE
                           || column.getUpdated()) {
                    // 在update操作时，oracle和mysql存放变更的非主键值的方式不同,oracle只有变更的字段; mysql会把变更前和变更后的字段都发出来，只需要取有变更的字段.
                    // 如果是oracle库，after里一定为对应的变更字段

                    boolean isUpdate = true;
                    if (entry.getHeader().getSourceType() == CanalEntry.Type.MYSQL) { // mysql的after里部分数据为未变更,oracle里after里为变更字段
                        isUpdate = column.getUpdated();
                    }
                    notKeyColumns.put(column.getName(), copyEventColumn(column, rowMode || isUpdate));// 如果是rowMode，所有字段都为updated
                }
            }

            if (entry.getHeader().getSourceType() == CanalEntry.Type.ORACLE) { // 针对oracle进行特殊处理
                checkUpdateKeyColumns(oldKeyColumns, keyColumns);
            }
        }

        if (keyColumns.isEmpty()) {
            throw new ClaveException("ERROR ## this rowdata has no pks , entry: " + entry.toString() + " and rowData: "
                                     + rowData);
        }

        List<EventColumn> keys = new ArrayList<EventColumn>(keyColumns.values());
        List<EventColumn> oldKeys = new ArrayList<EventColumn>(oldKeyColumns.values());
        List<EventColumn> columns = new ArrayList<EventColumn>(notKeyColumns.values());
        Collections.sort(keys, new EventColumnIndexComparable());
        Collections.sort(oldKeys, new EventColumnIndexComparable());
        Collections.sort(columns, new EventColumnIndexComparable());

        eventData.setKeys(keys);
        if (eventData.getEventType().isUpdate() && !oldKeys.equals(keys)) { // update类型，如果存在主键不同,则记录下old keys为变更前的主键
            eventData.setOldKeys(oldKeys);
        }
        eventData.setColumns(columns);
        return eventData;
    }

    /**
     * 在oracle中，补充没有变更的主键<br>
     * 如果变更后的主键为空，直接从old中拷贝<br>
     * 如果变更前后的主键数目不相等，把old中存在而new中不存在的主键拷贝到new中.
     * 
     * @param oldKeys
     * @param newKeys
     */
    private void checkUpdateKeyColumns(Map<String, EventColumn> oldKeyColumns, Map<String, EventColumn> keyColumns) {
        // 在变更前没有主键的情况
        if (oldKeyColumns.size() == 0) {
            return;
        }
        // 变更后的主键数据大于变更前的，不符合
        if (keyColumns.size() > oldKeyColumns.size()) {
            return;
        }
        // 主键没有变更，把所有变更前的主键拷贝到变更后的主键中.
        if (keyColumns.size() == 0) {
            keyColumns.putAll(oldKeyColumns);
            return;
        }

        // 把old中存在而new中不存在的主键拷贝到new中
        if (oldKeyColumns.size() != keyColumns.size()) {
            for (String oldKey : oldKeyColumns.keySet()) {
                if (keyColumns.get(oldKey) == null) {
                    keyColumns.put(oldKey, oldKeyColumns.get(oldKey));
                }
            }
        }
    }

    /**
     * 把 canal-protocol's Column 转化成 otter's model EventColumn.
     * 
     * @param column
     * @return
     */
    private EventColumn copyEventColumn(Column column, boolean isUpdate) {
        EventColumn eventColumn = new EventColumn();
        eventColumn.setIndex(column.getIndex());
        eventColumn.setKey(column.getIsKey());
        eventColumn.setNull(column.getIsNull());
        eventColumn.setColumnName(column.getName());
        eventColumn.setColumnType(column.getSqlType());
        eventColumn.setColumnValue(column.getValue());
        eventColumn.setUpdate(isUpdate);
        return eventColumn;
    }

    public void setRowMode(boolean rowMode) {
        this.rowMode = rowMode;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

}
