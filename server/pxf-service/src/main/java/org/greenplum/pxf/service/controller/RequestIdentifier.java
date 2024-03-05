package org.greenplum.pxf.service.controller;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.greenplum.pxf.api.model.RequestContext;

@Getter
@EqualsAndHashCode
@ToString
public class RequestIdentifier {
    private final String transactionId;
    private final int segmentId;
    private final String schemaName;
    private final String tableName;
    private final int remotePort;
    private final String profile;
    private final String server;

    public RequestIdentifier(RequestContext context) {
        this.transactionId = context.getTransactionId();
        this.segmentId = context.getSegmentId();
        this.schemaName = context.getSchemaName();
        this.tableName = context.getTableName();
        this.remotePort = context.getClientPort();
        this.profile = context.getProfile();
        this.server = context.getServerName();
    }
}
