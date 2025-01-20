package org.greenplum.pxf.automation;

public enum JdbcDbType {
    MYSQL("mysql"),
    ORACLE("oracle"),
    POSTGRES("default");

    private final String server;

    JdbcDbType(final String server) {
        this.server = server;
    }

    public String getServer() {
        return server;
    }
}