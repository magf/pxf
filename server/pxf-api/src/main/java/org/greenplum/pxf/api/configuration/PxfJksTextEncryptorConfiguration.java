package org.greenplum.pxf.api.configuration;

import io.arenadata.security.encryption.client.configuration.JksTextEncryptorConfiguration;
import org.springframework.beans.factory.annotation.Value;

public class PxfJksTextEncryptorConfiguration extends JksTextEncryptorConfiguration {
    @Value("${pxf.ssl.jks-store.path}")
    private String path;

    @Value("${pxf.ssl.jks-store.password}")
    private String password;

    @Value("${pxf.ssl.salt.key}")
    private String key;

    @Override
    protected String jksStorePath() {
        return path;
    }

    @Override
    protected char[] jksStorePassword() {
        return password.toCharArray();
    }

    @Override
    protected String secretKeyAlias() {
        return key;
    }
}
