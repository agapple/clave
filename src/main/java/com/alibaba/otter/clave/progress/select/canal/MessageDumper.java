package com.alibaba.otter.clave.progress.select.canal;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.clave.model.EventColumn;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.select.Message;

/**
 * dump记录
 * 
 * @author jianghang 2013-3-28 下午09:57:42
 * @version 1.0.0
 */
public class MessageDumper {

    private static final String SEP                    = SystemUtils.LINE_SEPARATOR;
    private static final String TIMESTAMP_FORMAT       = "yyyy-MM-dd HH:mm:ss:SSS";
    private static String       context_format         = null;
    private static String       eventData_format       = null;
    private static int          event_default_capacity = 1024;                      // 预设值StringBuilder，减少扩容影响

    static {
        context_format = "* Batch Id: [{0}] ,total : [{1}] , normal : [{2}] , filter :[{3}] , Time : {4}" + SEP;
        context_format += "* Start : [{5}] " + SEP;
        context_format += "* End : [{6}] " + SEP;

        eventData_format = "-----------------" + SEP;
        eventData_format += "- Schema: {0} , Table: {1} " + SEP;
        eventData_format += "- Type: {2}  , ExecuteTime: {3} " + SEP;
        eventData_format += "-----------------" + SEP;
        eventData_format += "---START" + SEP;
        eventData_format += "---Pks" + SEP;
        eventData_format += "{4}" + SEP;
        eventData_format += "---oldPks" + SEP;
        eventData_format += "{5}" + SEP;
        eventData_format += "---Columns" + SEP;
        eventData_format += "{6}" + SEP;
        eventData_format += "---END" + SEP;

    }

    public static String dumpMessageInfo(Message<EventData> message, String startPosition, String endPosition, int total) {
        Date now = new Date();
        SimpleDateFormat format = new SimpleDateFormat(TIMESTAMP_FORMAT);
        int normal = message.getDatas().size();
        return MessageFormat.format(context_format, String.valueOf(message.getId()), total, normal, total - normal,
                                    format.format(now), startPosition, endPosition);
    }

    public static String dumpEventDatas(List<EventData> eventDatas) {
        if (CollectionUtils.isEmpty(eventDatas)) {
            return StringUtils.EMPTY;
        }

        // 预先设定容量大小
        StringBuilder builder = new StringBuilder(event_default_capacity * eventDatas.size());
        for (EventData data : eventDatas) {
            builder.append(dumpEventData(data));
        }
        return builder.toString();
    }

    public static String dumpEventData(EventData eventData) {
        return MessageFormat.format(eventData_format, eventData.getSchemaName(), eventData.getTableName(),
                                    eventData.getEventType().getValue(), String.valueOf(eventData.getExecuteTime()),
                                    dumpEventColumn(eventData.getKeys()), dumpEventColumn(eventData.getOldKeys()),
                                    dumpEventColumn(eventData.getColumns()), "\t" + eventData.getSql());
    }

    private static String dumpEventColumn(List<EventColumn> columns) {
        StringBuilder builder = new StringBuilder(event_default_capacity);
        int size = columns.size();
        for (int i = 0; i < size; i++) {
            EventColumn column = columns.get(i);
            builder.append("\t").append(column.toString());
            if (i < columns.size() - 1) {
                builder.append(SEP);
            }
        }
        return builder.toString();
    }
}
