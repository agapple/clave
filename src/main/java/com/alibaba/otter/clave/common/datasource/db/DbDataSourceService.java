package com.alibaba.otter.clave.common.datasource.db;

import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.util.Assert;

import com.alibaba.otter.clave.common.datasource.DataMediaSource;
import com.alibaba.otter.clave.common.datasource.DataMediaType;
import com.alibaba.otter.clave.common.datasource.DataSourceService;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

/**
 * 基于数据库的链接实现
 * 
 * @author jianghang 2013-3-28 下午11:02:59
 * @version 1.0.0
 */
public class DbDataSourceService implements DataSourceService, DisposableBean {

    private static final Logger            logger                        = LoggerFactory.getLogger(DbDataSourceService.class);

    private int                            maxWait                       = 60 * 1000;

    private int                            minIdle                       = 0;

    private int                            initialSize                   = 0;

    private int                            maxActive                     = 32;

    private int                            maxIdle                       = 32;

    private int                            numTestsPerEvictionRun        = -1;

    private int                            timeBetweenEvictionRunsMillis = 60 * 1000;

    private int                            removeAbandonedTimeout        = 5 * 60;

    private int                            minEvictableIdleTimeMillis    = 5 * 60 * 1000;

    private Map<DbMediaSource, DataSource> dataSources;

    public DbDataSourceService(){
        dataSources = new MapMaker().makeComputingMap(new Function<DbMediaSource, DataSource>() {

            public DataSource apply(DbMediaSource dbMediaSource) {
                return createDataSource(dbMediaSource.getUrl(), dbMediaSource.getUsername(),
                                        dbMediaSource.getPassword(), dbMediaSource.getDriver(),
                                        dbMediaSource.getType(), dbMediaSource.getEncode());

            }
        });

    }

    public DataSource getDataSource(DataMediaSource dataMediaSource) {
        Assert.notNull(dataMediaSource);
        return dataSources.get(dataMediaSource);
    }

    public void destroy(DataMediaSource dataMediaSource) {
        DataSource source = dataSources.remove(dataMediaSource);
        if (source != null) {
            try {
                // fallback for regular destroy
                BasicDataSource basicDataSource = (BasicDataSource) source;
                basicDataSource.close();
            } catch (SQLException e) {
                logger.error("ERROR ## close the datasource has an error", e);
            }
        }
    }

    public void destroy() {
        for (DataSource source : dataSources.values()) {
            if (source != null) {
                try {
                    // fallback for regular destroy
                    BasicDataSource basicDataSource = (BasicDataSource) source;
                    basicDataSource.close();
                } catch (SQLException e) {
                    logger.error("ERROR ## close the datasource has an error", e);
                }
            }
        }
    }

    private DataSource createDataSource(String url, String userName, String password, String driverClassName,
                                        DataMediaType dataMediaType, String encoding) {

        return doCreateDataSource(url, userName, password, driverClassName, dataMediaType, encoding);
    }

    @SuppressWarnings("deprecation")
    protected DataSource doCreateDataSource(String url, String userName, String password, String driverClassName,
                                            DataMediaType dataMediaType, String encoding) {
        BasicDataSource dbcpDs = new BasicDataSource();

        dbcpDs.setInitialSize(initialSize);// 初始化连接池时创建的连接数
        dbcpDs.setMaxActive(maxActive);// 连接池允许的最大并发连接数，值为非正数时表示不限制
        dbcpDs.setMaxIdle(maxIdle);// 连接池中的最大空闲连接数，超过时，多余的空闲连接将会被释放，值为负数时表示不限制
        dbcpDs.setMinIdle(minIdle);// 连接池中的最小空闲连接数，低于此数值时将会创建所欠缺的连接，值为0时表示不创建
        dbcpDs.setMaxWait(maxWait);// 以毫秒表示的当连接池中没有可用连接时等待可用连接返回的时间，超时则抛出异常，值为-1时表示无限等待
        dbcpDs.setRemoveAbandoned(true);// 是否清除已经超过removeAbandonedTimeout设置的无效连接
        dbcpDs.setLogAbandoned(true);// 当清除无效链接时是否在日志中记录清除信息的标志
        dbcpDs.setRemoveAbandonedTimeout(removeAbandonedTimeout); // 以秒表示清除无效链接的时限
        dbcpDs.setNumTestsPerEvictionRun(numTestsPerEvictionRun);// 确保连接池中没有已破损的连接
        dbcpDs.setTestOnBorrow(false);// 指定连接被调用时是否经过校验
        dbcpDs.setTestOnReturn(false);// 指定连接返回到池中时是否经过校验
        dbcpDs.setTestWhileIdle(true);// 指定连接进入空闲状态时是否经过空闲对象驱逐进程的校验
        dbcpDs.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis); // 以毫秒表示空闲对象驱逐进程由运行状态进入休眠状态的时长，值为非正数时表示不运行任何空闲对象驱逐进程
        dbcpDs.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis); // 以毫秒表示连接被空闲对象驱逐进程驱逐前在池中保持空闲状态的最小时间

        // 动态的参数
        dbcpDs.setDriverClassName(driverClassName);
        dbcpDs.setUrl(url);
        dbcpDs.setUsername(userName);
        dbcpDs.setPassword(password);

        if (dataMediaType.isOracle()) {
            dbcpDs.addConnectionProperty("restrictGetTables", "true");
            dbcpDs.setValidationQuery("select 1 from dual");
        } else if (dataMediaType.isMysql()) {
            // open the batch mode for mysql since 5.1.8
            dbcpDs.addConnectionProperty("useServerPrepStmts", "false");
            dbcpDs.addConnectionProperty("rewriteBatchedStatements", "true");
            if (StringUtils.isNotEmpty(encoding)) {
                dbcpDs.addConnectionProperty("characterEncoding", encoding);
            }
            dbcpDs.setValidationQuery("select 1");
        } else {
            logger.error("ERROR ## Unknow database type");
        }

        return dbcpDs;
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public void setTimeBetweenEvictionRunsMillis(int timeBetweenEvictionRunsMillis) {
        this.timeBetweenEvictionRunsMillis = timeBetweenEvictionRunsMillis;
    }

    public void setRemoveAbandonedTimeout(int removeAbandonedTimeout) {
        this.removeAbandonedTimeout = removeAbandonedTimeout;
    }

    public void setMinEvictableIdleTimeMillis(int minEvictableIdleTimeMillis) {
        this.minEvictableIdleTimeMillis = minEvictableIdleTimeMillis;
    }

}
