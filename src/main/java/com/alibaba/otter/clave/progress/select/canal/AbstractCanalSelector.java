package com.alibaba.otter.clave.progress.select.canal;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.CollectionUtils;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.clave.ClaveConfig;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.select.ClaveSelector;
import com.alibaba.otter.clave.progress.select.Message;

public abstract class AbstractCanalSelector implements ClaveSelector<EventData> {

    protected static final Logger logger       = LoggerFactory.getLogger(AbstractCanalSelector.class);
    protected static final String SEP          = SystemUtils.LINE_SEPARATOR;
    protected static final String DATE_FORMAT  = "yyyy-MM-dd HH:mm:ss";
    protected String              destination;
    protected int                 logSplitSize = 50;
    protected boolean             dump         = true;
    protected boolean             dumpDetail   = true;
    protected MessageParser       messageParser;

    protected Message<EventData> selector(com.alibaba.otter.canal.protocol.Message message) throws InterruptedException {
        List<EventData> eventDatas = messageParser.parse(message.getEntries()); // 过滤事务头/尾和回环数据
        Message<EventData> result = new Message<EventData>(message.getId(), eventDatas);

        if (dump && logger.isInfoEnabled()) {
            String startPosition = null;
            String endPosition = null;
            if (!CollectionUtils.isEmpty(message.getEntries())) {
                startPosition = buildPositionForDump(message.getEntries().get(0));
                endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
            }

            dumpMessages(result, startPosition, endPosition, message.getEntries().size());// 记录一下，方便追查问题
        }
        return result;
    }

    /**
     * 记录一下message对象
     */
    protected void dumpMessages(Message message, String startPosition, String endPosition, int total) {
        try {
            MDC.put(ClaveConfig.splitLogFileKey, destination);
            logger.info(SEP + "****************************************************" + SEP);
            logger.info(MessageDumper.dumpMessageInfo(message, startPosition, endPosition, total));
            logger.info("****************************************************" + SEP);
            if (dumpDetail) {// 判断一下是否需要打印详细信息
                dumpEventDatas(message.getDatas());
                logger.info("****************************************************" + SEP);
            }
        } finally {
            MDC.remove(ClaveConfig.splitLogFileKey);
        }
    }

    /**
     * 分批输出多个数据
     */
    protected void dumpEventDatas(List<EventData> eventDatas) {
        int size = eventDatas.size();
        // 开始输出每条记录
        int index = 0;
        do {
            if (index + logSplitSize >= size) {
                logger.info(MessageDumper.dumpEventDatas(eventDatas.subList(index, size)));
            } else {
                logger.info(MessageDumper.dumpEventDatas(eventDatas.subList(index, index + logSplitSize)));
            }
            index += logSplitSize;
        } while (index < size);
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
               + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    public void setLogSplitSize(int logSplitSize) {
        this.logSplitSize = logSplitSize;
    }

    public void setDump(boolean dump) {
        this.dump = dump;
    }

    public void setDumpDetail(boolean dumpDetail) {
        this.dumpDetail = dumpDetail;
    }

    public void setMessageParser(MessageParser messageParser) {
        this.messageParser = messageParser;
    }

}
