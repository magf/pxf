package org.greenplum.pxf.api.configuration;

import io.arenadata.security.encryption.client.configuration.JksTextEncryptorConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty({"pxf.ssl.jks-store.path", "pxf.ssl.jks-store.password", "pxf.ssl.salt.key"})
public class PxfJksTextEncryptorConfiguration extends JksTextEncryptorConfiguration {
    private final String path;
    private final String password;
    private final String key;

    public PxfJksTextEncryptorConfiguration(@Value("${pxf.ssl.jks-store.path}") String path,
                                            @Value("${pxf.ssl.jks-store.password}") String password,
                                            @Value("${pxf.ssl.salt.key}") String key) {
        this.path = path;
        this.password = password;
        this.key = key;
    }

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
