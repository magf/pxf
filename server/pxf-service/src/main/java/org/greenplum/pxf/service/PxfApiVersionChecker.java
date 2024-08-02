package org.greenplum.pxf.service;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

/**
 * Checks if two API versions are compatible
 */
@Component
public class PxfApiVersionChecker {

    /**
     * @param serverApiVersion - the server API version
     * @param clientApiVersion - the client API version
     * @return true if the server is compatible with the client's API version
     */
    public boolean isCompatible(String serverApiVersion, String clientApiVersion) {
        return StringUtils.equalsIgnoreCase(serverApiVersion, clientApiVersion);
    }
}
