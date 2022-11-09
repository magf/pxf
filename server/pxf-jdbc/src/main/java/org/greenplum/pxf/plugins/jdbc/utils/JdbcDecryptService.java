package org.greenplum.pxf.plugins.jdbc.utils;

import io.arenadata.security.encryption.client.provider.TextEncryptorProvider;
import io.arenadata.security.encryption.client.service.DecryptClient;
import org.greenplum.pxf.api.configuration.PxfJksTextEncryptorConfiguration;

public class JdbcDecryptService {
    private final PxfJksTextEncryptorConfiguration configuration;

    public JdbcDecryptService(PxfJksTextEncryptorConfiguration configuration) {
        this.configuration = configuration;
    }

    public String decrypt(String encryptedPassword) {
        TextEncryptorProvider provider = configuration.textEncryptorProvider();
        DecryptClient decryptClient = configuration.decryptService(provider);
        return decryptClient.decrypt(encryptedPassword);
    }
}
