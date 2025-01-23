package org.greenplum.pxf.automation;

public enum JdbcDbType {
    MYSQL("mysql"),
    ORACLE("oracle"),
    POSTGRES("default");

    private final String serverName;


    JdbcDbType(final String serverName) {
        this.serverName = serverName;
    }

    public String getServerName() {
        return serverName;
    }
}