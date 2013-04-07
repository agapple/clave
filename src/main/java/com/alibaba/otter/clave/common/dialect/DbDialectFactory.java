package com.alibaba.otter.clave.common.dialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import com.alibaba.otter.clave.common.datasource.DataSourceService;
import com.alibaba.otter.clave.common.datasource.db.DbMediaSource;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;

/**
 * @author jianghang 2011-10-27 下午02:12:06
 * @version 1.0.0
 */
public class DbDialectFactory implements DisposableBean {

    private static final Logger           logger = LoggerFactory.getLogger(DbDialectFactory.class);
    private DataSourceService             dataSourceService;
    private DbDialectGenerator            dbDialectGenerator;

    private Map<DbMediaSource, DbDialect> dialects;

    public DbDialectFactory(){
        dialects = new MapMaker().makeComputingMap(new Function<DbMediaSource, DbDialect>() {

            public DbDialect apply(final DbMediaSource source) {
                DataSource dataSource = dataSourceService.getDataSource(source);
                final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
                return (DbDialect) jdbcTemplate.execute(new ConnectionCallback() {

                    public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                        DatabaseMetaData meta = c.getMetaData();
                        String databaseName = meta.getDatabaseProductName();
                        int databaseMajorVersion = meta.getDatabaseMajorVersion();
                        int databaseMinorVersion = meta.getDatabaseMinorVersion();
                        DbDialect dialect = dbDialectGenerator.generate(jdbcTemplate, databaseName,
                                                                        databaseMajorVersion, databaseMinorVersion,
                                                                        source.getType());
                        if (dialect == null) {
                            throw new UnsupportedOperationException("no dialect for" + databaseName);
                        }

                        if (logger.isInfoEnabled()) {
                            logger.info(String.format(
                                                      "--- DATABASE: %s, SCHEMA: %s ---",
                                                      databaseName,
                                                      (dialect.getDefaultSchema() == null) ? dialect.getDefaultCatalog() : dialect.getDefaultSchema()));
                        }

                        return dialect;
                    }
                });

            }
        });

    }

    public DbDialect getDbDialect(DbMediaSource source) {
        return dialects.get(source);
    }

    public void destory(DbMediaSource source) {
        DbDialect dialect = dialects.remove(source);
        if (dialect != null) {
            dialect.destory();
        }
    }

    public void destroy() throws Exception {
        for (DbDialect dialect : dialects.values()) {
            dialect.destory();
        }
    }

    // =============== setter / getter =================

    public void setDataSourceService(DataSourceService dataSourceService) {
        this.dataSourceService = dataSourceService;
    }

    public void setDbDialectGenerator(DbDialectGenerator dbDialectGenerator) {
        this.dbDialectGenerator = dbDialectGenerator;
    }

}
