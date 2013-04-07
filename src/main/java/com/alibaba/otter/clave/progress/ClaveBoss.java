package com.alibaba.otter.clave.progress;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.clave.common.lifecycle.AbstractClaveLifeCycle;
import com.alibaba.otter.clave.model.EventData;
import com.alibaba.otter.clave.model.RowBatch;
import com.alibaba.otter.clave.progress.select.ClaveSelector;
import com.alibaba.otter.clave.progress.select.Message;

/**
 * progress的控制线程
 * 
 * @author jianghang 2013-4-7 下午03:08:48
 * @version 1.0.0
 */
public class ClaveBoss extends AbstractClaveLifeCycle {

    private static final Logger      logger = LoggerFactory.getLogger(ClaveBoss.class);
    private ClaveSelector<EventData> select;
    private ClaveProgress<RowBatch>  progress;
    private ExecutorService          executor;

    public void start() {
        if (!select.isStart()) {
            select.start();
        }

        if (!progress.isStart()) {
            progress.start();
        }

        executor = Executors.newFixedThreadPool(1);
        executor.submit(new Runnable() {

            public void run() {
                process();
            }
        });

        super.start();
    }

    public void stop() {
        super.stop();
        executor.shutdownNow();

        if (select.isStart()) {
            select.stop();
        }

        if (progress.isStart()) {
            progress.stop();
        }

    }

    public void process() {
        AtomicBoolean rollback = new AtomicBoolean(true);
        while (isStart()) {
            try {
                if (rollback.compareAndSet(true, false)) {
                    select.rollback();
                }

                // 获取数据
                Message<EventData> message = select.selector();

                RowBatch rowBatch = new RowBatch();
                // 进行数据合并
                for (EventData data : message.getDatas()) {
                    rowBatch.merge(data);
                }

                boolean result = progress.process(rowBatch);
                if (result) {
                    select.ack(message.getId());
                } else {
                    select.rollback(message.getId());
                }
            } catch (Exception e) {
                logger.error("process error!", e);
                rollback.compareAndSet(false, true);
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e1) {
                    // ignore
                }
            }
        }
    }

    public void setSelect(ClaveSelector<EventData> select) {
        this.select = select;
    }

    public void setProgress(ClaveProgress<RowBatch> progress) {
        this.progress = progress;
    }

}
