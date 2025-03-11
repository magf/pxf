package org.greenplum.pxf.automation.utils.system;

import jdk.internal.joptsimple.internal.Strings;
import org.apache.commons.lang.StringUtils;

public class VaultIntegrationTools {
    public static final boolean IS_VAULT_ENABLED = !StringUtils.isEmpty(System.getProperty("PXF_VAULT_ENABLED"))
            && Boolean.parseBoolean(System.getProperty("PXF_VAULT_ENABLED"));
}
