package com.alibaba.otter.clave;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.otter.clave.progress.ClaveBoss;

/**
 * spring容器获取
 * 
 * @author jianghang 2012-2-20 下午09:05:59
 * @version 1.0.0
 */
public class ClaveLocator {

    private static ApplicationContext context       = null;
    private static RuntimeException   initException = null;

    static {
        try {
            context = new ClassPathXmlApplicationContext("spring/clave.xml");
        } catch (RuntimeException e) {
            throw e;
        }
    }

    public static ApplicationContext getApplicationContext() {
        if (context == null) {
            throw initException;
        }

        return context;
    }

    public static void close() {
        ((ClassPathXmlApplicationContext) context).close();
    }

    public static ClaveBoss getClaveBoss() {
        return (ClaveBoss) getApplicationContext().getBean("boss");
    }

}
