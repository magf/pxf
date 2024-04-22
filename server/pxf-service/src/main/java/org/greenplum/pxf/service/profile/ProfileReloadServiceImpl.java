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
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.Reloader;
import org.greenplum.pxf.service.controller.ReadService;
import org.greenplum.pxf.service.controller.WriteService;
import org.greenplum.pxf.service.rest.dto.ProfileReloadRequestDto;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

@Slf4j
@Component
public class ProfileReloadServiceImpl implements ProfileReloadService {
    private final ReadService readService;
    private final WriteService writeService;
    private final Map<String, Reloader> profileReloaderMap;

    public ProfileReloadServiceImpl(ReadService readService,
                                    WriteService writeService,
                                    @Lazy Map<String, Reloader> profileReloaderMap) {
        this.readService = readService;
        this.writeService = writeService;
        this.profileReloaderMap = profileReloaderMap;
    }

    @Override
    public void reloadProfile(ProfileReloadRequestDto reloadRequestDto) {
        String profile = reloadRequestDto.getProfile() == null ? null : reloadRequestDto.getProfile().toLowerCase();
        String server = reloadRequestDto.getServer();
        log.info("Received a request to reload a profile with the parameters: profile={}, server={}", profile, server);
        if (StringUtils.isBlank(server)) {
            reloadAll(profile);
        } else if (StringUtils.isNotBlank(profile)) {
            reload(profile, server);
        } else {
            String message = String.format(String.format("The provided parameters (profile=%s, server=%s) " +
                    "are not correct. Please add profile", profile, server));
            log.error(message);
            throw new IllegalArgumentException(message);
        }
    }

    private void reloadAll(String profile) {
        Predicate<String> profilePredicate = key -> (StringUtils.isBlank(profile) || key.equals(profile));
        profileReloaderMap.forEach((profileName, reloader) -> {
            if (profilePredicate.test(profileName)) {
                cancelQueryExecutions(profileName, null);
                log.info("Canceled running queries with profile '{}' for all servers", profileName);
                reloader.reloadAll();
                log.info("Reloaded profile '{}' for all servers with reloader {}", profileName, reloader);
            } else {
                String message = String.format(
                        "Profile '%s' doesn't support reloading methods. Skipping reloading for all servers. " +
                                "Profiles with supporting reloading methods: %s", profile, profileReloaderMap.keySet());
                log.error(message);
                throw new PxfRuntimeException(message);
            }
        });
    }

    private void reload(String profile, String server) {
        Reloader reloader = profileReloaderMap.get(profile);
        if (Objects.nonNull(reloader)) {
            cancelQueryExecutions(profile, server);
            log.info("Canceled running queries with profile '{}' and server '{}'", profile, server);
            reloader.reload(server);
            log.info("Reloaded profile '{}' for server '{}' with reloader {}", profile, server, reloader);
        } else {
            String message = String.format(
                    "Profile '%s' doesn't support reloading methods. Skipping reloading for server '%s'. " +
                            "Profiles with supporting reloading methods: %s", profile, server, profileReloaderMap.keySet());
            log.error(message);
            throw new PxfRuntimeException(message);
        }
    }

    private void cancelQueryExecutions(String profile, String server) {
        readService.cancelReadExecutions(profile, server);
        writeService.cancelWriteExecutions(profile, server);
    }
}
