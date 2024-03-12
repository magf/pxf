package org.greenplum.pxf.api.model;

public interface CancelableOperation {
    /**
     * Cancel read operation. Might contain additional logic comparing with {@link Accessor#closeForRead()}
     *
     * @throws Exception if the cancel operation failed
     */
    void cancelRead() throws Exception;

    /**
     * Cancel write operation. Might contain additional logic comparing with {@link Accessor#closeForWrite()}
     *
     * @throws Exception if the cancel the operation failed
     */
    void cancelWrite() throws Exception;
}
