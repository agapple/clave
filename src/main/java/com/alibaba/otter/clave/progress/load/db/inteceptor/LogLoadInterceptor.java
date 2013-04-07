package com.alibaba.otter.clave.progress.load.db.inteceptor;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.load.db.DbLoadContext;
import com.alibaba.otter.clave.progress.load.db.DbLoadDumper;
import com.alibaba.otter.clave.progress.load.interceptor.AbstractLoadInterceptor;

/**
 * load的日志记录
 * 
 * @author jianghang 2011-11-10 上午11:31:05
 * @version 4.0.0
 */
public class LogLoadInterceptor extends AbstractLoadInterceptor<DbLoadContext, EventData> {

    private static final Logger logger           = LoggerFactory.getLogger(LogLoadInterceptor.class);
    private static final String SEP              = SystemUtils.LINE_SEPARATOR;
    private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss:SSS";
    private int                 batchSize        = 50;
    private static String       context_format   = null;
    private boolean             dump             = true;

    static {
        context_format = "* status : {0}  , time : {1} *" + SEP;
        context_format += "* total Data : [{2}] , success Data : [{3}] , failed Data : [{4}]]" + SEP;
    }

    public void commit(DbLoadContext context) {
        // 成功时记录一下
        if (dump && logger.isInfoEnabled()) {
            synchronized (LogLoadInterceptor.class) {
                logger.info(SEP + "****************************************************" + SEP);
                logger.info(dumpContextInfo("successed", context));
                logger.info("****************************************************" + SEP);
                logger.info("* process Data  *" + SEP);
                logEventDatas(context.getProcessedDatas());
                logger.info("-----------------" + SEP);
                logger.info("* failed Data *" + SEP);
                logEventDatas(context.getFailedDatas());
                logger.info("****************************************************" + SEP);
            }
        }
    }

    public void error(DbLoadContext context) {
        if (dump && logger.isInfoEnabled()) {
            synchronized (LogLoadInterceptor.class) {
                logger.info(dumpContextInfo("error", context));
                logger.info("* process Data  *" + SEP);
                logEventDatas(context.getProcessedDatas());
                logger.info("-----------------" + SEP);
                logger.info("* failed Data *" + SEP);
                logEventDatas(context.getFailedDatas());
                logger.info("****************************************************" + SEP);
            }
        }
    }

    /**
     * 分批输出多个数据
     */
    private void logEventDatas(List<EventData> eventDatas) {
        int size = eventDatas.size();
        // 开始输出每条记录
        int index = 0;
        do {
            if (index + batchSize >= size) {
                logger.info(DbLoadDumper.dumpEventDatas(eventDatas.subList(index, size)));
            } else {
                logger.info(DbLoadDumper.dumpEventDatas(eventDatas.subList(index, index + batchSize)));
            }
            index += batchSize;
        } while (index < size);
    }

    private String dumpContextInfo(String status, DbLoadContext context) {
        int successed = context.getProcessedDatas().size();
        int failed = context.getFailedDatas().size();
        int all = context.getPrepareDatas().size();
        Date now = new Date();
        SimpleDateFormat format = new SimpleDateFormat(TIMESTAMP_FORMAT);
        return MessageFormat.format(context_format, status, format.format(now), all, successed, failed);
    }

    public void setDump(boolean dump) {
        this.dump = dump;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

}
