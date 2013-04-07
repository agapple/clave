package com.alibaba.otter.clave;

public class ClaveConfig {

    /**
     * 在logback的配置文件中定义好进行日志文件输出的键值.
     */
    public static String splitLogFileKey               = "clave";

    public static String SYSTEM_SCHEMA                 = "retl";
    public static String SYSTEM_MARK_TABLE             = "retl_mark";
    public static String SYSTEM_BUFFER_TABLE           = "retl_buffer";
    public static String SYSTEM_DUAL_TABLE             = "xdual";
    public static String SYSTEM_TABLE_MARK_INFO_COLUMN = "server_info";
    public static String SYSTEM_TABLE_MARK_ID_COLUMN   = "server_id";

}
