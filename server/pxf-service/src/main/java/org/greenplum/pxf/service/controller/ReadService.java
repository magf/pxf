package org.greenplum.pxf.service.controller;

import org.greenplum.pxf.api.model.RequestContext;

import java.io.OutputStream;

/**
 * Service that reads data from external systems.
 */
public interface ReadService {

    /**
     * Reads data from the external system specified by the RequestContext.
     * The data is then written to the provided OutputStream.
     *
     * @param context      request context
     * @param outputStream output stream to write data to
     */
    void readData(RequestContext context, OutputStream outputStream);

    /**
     * Tries to cancel active read request to the external system specified by the RequestContext.
     * Returns true if read request was found and cancelled, false otherwise
     *
     * @param context request context
     * @return if read request cancellation succeeded
     */
    boolean cancelRead(RequestContext context);
}
