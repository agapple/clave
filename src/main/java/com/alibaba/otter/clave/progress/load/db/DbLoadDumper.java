package com.alibaba.otter.clave.progress.load.db;

import java.text.MessageFormat;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.clave.model.EventColumn;
import com.alibaba.otter.clave.model.EventData;

/**
 * dump记录
 * 
 * @author jianghang 2011-11-9 下午03:52:26
 * @version 1.0.0
 */
public class DbLoadDumper {

    private static final String SEP                    = SystemUtils.LINE_SEPARATOR;

    private static String       context_format         = null;
    private static String       eventData_format       = null;
    private static int          event_default_capacity = 1024;                      // 预设值StringBuilder，减少扩容影响

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* total Data : [{0}] , success Data : [{1}] , failed Data : [{2}] " + SEP;
        context_format += "****************************************************" + SEP;
        context_format += "* process Data  *" + SEP;
        context_format += "{3}" + SEP;
        context_format += "****************************************************" + SEP;
        context_format += "* failed Data *" + SEP;
        context_format += "{4}" + SEP;
        context_format += "****************************************************" + SEP;

        eventData_format = "-----------------" + SEP;
        eventData_format += "- Schema: {0} , Table: {1} " + SEP;
        eventData_format += "- EventType : {2} , Time : {3} " + SEP;
        eventData_format += "-----------------" + SEP;
        eventData_format += "---Pks" + SEP;
        eventData_format += "{4}" + SEP;
        eventData_format += "---oldPks" + SEP;
        eventData_format += "{5}" + SEP;
        eventData_format += "---Columns" + SEP;
        eventData_format += "{6}" + SEP;
        eventData_format += "---Sql" + SEP;
        eventData_format += "{7}" + SEP;
    }

    public static String dumpContext(DbLoadContext context) {
        int successed = context.getProcessedDatas().size();
        int failed = context.getFailedDatas().size();
        int all = context.getPrepareDatas().size();
        return MessageFormat.format(context_format, all, successed, failed,
                                    dumpEventDatas(context.getProcessedDatas()),
                                    dumpEventDatas(context.getFailedDatas()));
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
                                    eventData.getEventType(), String.valueOf(eventData.getExecuteTime()),
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
