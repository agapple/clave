package com.alibaba.otter.clave.progress.select;

import org.junit.Test;

import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.progress.select.Message;
import com.alibaba.otter.clave.progress.select.canal.CanalClientSelector;
import com.alibaba.otter.clave.progress.select.canal.MessageParser;

public class CanalClientSelectorTest {

    @Test
    public void testSimple() throws Exception {
        CanalClientSelector selector = new CanalClientSelector("example", "10.20.144.51:2181");
        MessageParser messageParser = new MessageParser();
        selector.setMessageParser(messageParser);
        selector.setBatchSize(100);
        selector.setFilter("");
        selector.start();

        selector.rollback();
        int totalEmtryCount = 120;
        int emptyCount = 0;
        while (emptyCount < totalEmtryCount) {
            Message<EventData> message = selector.selector();
            long batchId = message.getId();
            int size = message.getDatas().size();
            if (batchId == -1 || size == 0) {
                emptyCount++;
                System.out.println("empty count : " + emptyCount);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            } else {
                emptyCount = 0;
                System.out.printf("message[batchId=%s,size=%s] \n", batchId, size);
            }

            selector.ack(batchId); // 提交确认
            // connector.rollback(batchId); // 处理失败, 回滚数据
        }

        System.out.println("empty too many times, exit");
        selector.stop();
    }
}
