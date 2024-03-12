package org.greenplum.pxf.service.profile;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import lombok.extern.slf4j.Slf4j;
import org.greenplum.pxf.api.model.Plugin;
import org.greenplum.pxf.api.model.Reloader;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Configuration
public class ProfileReloadConfiguration {
    private final static String ACCESSOR_KEY = "ACCESSOR";

    @Lazy
    @Bean("profileReloaderMap")
    public Map<String, Reloader> getProfileReloaderMap(ProfilesConf profileConf, BasePluginFactory basePluginFactory) {
        Map<String, Reloader> profileReloaderMap = new HashMap<>();
        profileConf.getProfilesMap().keySet()
                .forEach(profile -> getReloader(profile, profileConf, basePluginFactory)
                        .ifPresent(reloader -> profileReloaderMap.put(profile, reloader)));
        return profileReloaderMap;
    }

    private Optional<Reloader> getReloader(String profile,
                                           ProfilesConf profileConf,
                                           BasePluginFactory basePluginFactory) {
        try {
            Map<String, String> pluginMap = profileConf.getPlugins(profile);
            return Optional.ofNullable(pluginMap)
                    .map(plugins -> getReloaderInstance(plugins.get(ACCESSOR_KEY), basePluginFactory, profile));
        } catch (Exception e) {
            log.warn("Profile '{}': Failed to get plugin map", profile);
        }
        return Optional.empty();
    }

    private Reloader getReloaderInstance(String accessorClassName, BasePluginFactory basePluginFactory, String profile) {
        try {
            Plugin instance = basePluginFactory.getPluginInstance(accessorClassName);
            log.debug("Profile '{}': Initialize instance {} of the accessor class {}", profile, instance, accessorClassName);
            if (Reloader.class.isAssignableFrom(instance.getClass())) {
                return (Reloader) instance;
            } else {
                log.debug("Profile '{}': Accessor class {} doesn't implement Reloader interface", profile, accessorClassName);
            }
        } catch (Exception e) {
            log.warn("Profile '{}': Failed to initialize instance of the accessor class {}. {}",
                    profile, accessorClassName, e.getMessage());
        }
        return null;
    }
}
