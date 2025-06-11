package org.greenplum.pxf.automation.components.mysql;

import org.greenplum.pxf.automation.components.common.DbSystemObject;
import org.greenplum.pxf.automation.utils.jsystem.report.ReportUtils;

import java.sql.ResultSet;

/**
 * MySQL System Object
 */
public class Mysql extends DbSystemObject {

    private String password;

    public Mysql() {
    }

    public Mysql(boolean silenceReport) {
        super(silenceReport);
    }

    public String getPassword() {
        return password;
    }

    public String getDriver() {
        return driver;
    }

    @Override
    public void init() throws Exception {
        super.init();

        password = this.getProperty("password");
        if (userName == null) {
            userName = System.getProperty("user.name");
        }

        driver = "com.mysql.cj.jdbc.Driver";
        address = "jdbc:mysql://" + userName + ":" + password + "@" + getHost() + ":" + getPort() + "/" + db;

        connect();
    }


    @Override
    public void createDataBase(String schemaName, boolean ignoreFail, String encoding, String localeCollate, String localeCollateType) {
    }

    public Object getValueFromQuery(String query, int index, Class<?> clazz) throws Exception {
        ReportUtils.report(report, getClass(), "Get value - query: " + query);
        ResultSet res = stmt.executeQuery(query);
        if (res.next()) {
            Object value = res.getObject(index, clazz);
            ReportUtils.report(report, getClass(), "Value: [" + value + "]");
            return value;
        } else {
            throw new IllegalStateException("There is no any result of the query: " + query);
        }
    }
}
