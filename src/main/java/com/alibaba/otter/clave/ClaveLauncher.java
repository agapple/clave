package com.alibaba.otter.clave;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.clave.progress.ClaveBoss;

/**
 * 启动一个clave实例
 * 
 * @author jianghang 2012-2-20 下午09:04:09
 * @version 1.0.0
 */
public class ClaveLauncher {

    private static final Logger logger = LoggerFactory.getLogger(ClaveLauncher.class);

    public static void main(String[] args) {
        logger.info("INFO ## load the config");
        ClaveLocator.getApplicationContext();
        logger.info("INFO ## start clave ......");
        ClaveBoss boss = ClaveLocator.getClaveBoss();
        Runtime.getRuntime().addShutdownHook(new Thread() {

            public void run() {
                ClaveLocator.close();
            }

        });

        boss.start();
    }
}
