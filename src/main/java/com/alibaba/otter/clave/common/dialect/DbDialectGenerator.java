package com.alibaba.otter.clave.common.dialect;

import org.apache.commons.lang.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.LobHandler;

import com.alibaba.otter.clave.common.datasource.DataMediaType;
import com.alibaba.otter.clave.common.dialect.mysql.MysqlDialect;
import com.alibaba.otter.clave.common.dialect.oracle.OracleDialect;

/**
 * @author jianghang 2013-3-28 下午11:06:18
 * @version 1.0.0
 */
public class DbDialectGenerator {

    protected static final String ORACLE = "oracle";
    protected static final String MYSQL  = "mysql";

    protected LobHandler          defaultLobHandler;
    protected LobHandler          oracleLobHandler;

    protected DbDialect generate(JdbcTemplate jdbcTemplate, String databaseName, int databaseMajorVersion,
                                 int databaseMinorVersion, DataMediaType dataMediaType) {
        DbDialect dialect = null;

        if (StringUtils.startsWithIgnoreCase(databaseName, ORACLE)) { // for oracle
            dialect = new OracleDialect(jdbcTemplate, oracleLobHandler, databaseName, databaseMajorVersion,
                                        databaseMinorVersion);
        } else if (StringUtils.startsWithIgnoreCase(databaseName, MYSQL)) { // for mysql
            dialect = new MysqlDialect(jdbcTemplate, defaultLobHandler, databaseName, databaseMajorVersion,
                                       databaseMinorVersion);
        }

        return dialect;
    }

    // ======== setter =========
    public void setDefaultLobHandler(LobHandler defaultLobHandler) {
        this.defaultLobHandler = defaultLobHandler;
    }

    public void setOracleLobHandler(LobHandler oracleLobHandler) {
        this.oracleLobHandler = oracleLobHandler;
    }
}
