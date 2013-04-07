package com.alibaba.otter.clave.progress.load.db.inteceptor.operation;


/**
 * 基于erosa的日志记录
 * 
 * @author jianghang 2011-10-31 下午02:48:22
 * @version 4.0.0
 */
public class ErosaMysqlInterceptor extends AbstractOperationInterceptor {

    public static final String mergeofMysqlSql = "INSERT INTO {0} (id, {1}) VALUES (?, ?) ON DUPLICATE KEY UPDATE {1} = VALUES({1})";

    public ErosaMysqlInterceptor(){
        super(mergeofMysqlSql);
    }

}
