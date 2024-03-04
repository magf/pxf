package org.greenplum.pxf.api.model;

public interface Reloader {

    void reloadAll();

    void reload(String server);
}
