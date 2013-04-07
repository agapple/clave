package com.alibaba.otter.clave.common.datasource.db;

import java.util.Properties;

import com.alibaba.otter.clave.common.datasource.DataMediaSource;

/**
 * 基于db的source信息
 * 
 * @author jianghang 2013-3-28 下午10:55:02
 * @version 1.0.0
 */
public class DbMediaSource extends DataMediaSource {

    private static final long serialVersionUID = 2840851954936715456L;
    private String            url;
    private String            username;
    private String            password;
    private String            driver;
    private Properties        properties;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}
