package org.greenplum.pxf.service.spring;

import org.greenplum.pxf.api.configuration.PxfJksTextEncryptorConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PxfJksTextEncryptor {

    @Bean
    @ConditionalOnProperty({"pxf.ssl.jks-store.path", "pxf.ssl.jks-store.password", "pxf.ssl.salt.key"})
    PxfJksTextEncryptorConfiguration pxfJksTextEncryptorConfiguration() {
        return new PxfJksTextEncryptorConfiguration();
    }
}
