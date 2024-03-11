package org.greenplum.pxf.service.profile;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.Reloader;
import org.greenplum.pxf.service.rest.dto.ProfileReloadRequestDto;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

@Slf4j
@Component
public class ProfileReloadServiceImpl implements ProfileReloadService {
    private final Map<String, Reloader> profileReloaderMap;

    public ProfileReloadServiceImpl(@Lazy Map<String, Reloader> profileReloaderMap) {
        this.profileReloaderMap = profileReloaderMap;
    }

    @Override
    public void reloadProfile(ProfileReloadRequestDto reloadRequestDto) {
        String profile = reloadRequestDto.getProfile();
        String server = reloadRequestDto.getServer();
        log.info("Received a request to reload a profile with the parameters: profile={}, server={}", profile, server);
        if (StringUtils.isBlank(server)) {
            reloadAll(profile);
        } else if (StringUtils.isNotBlank(profile)) {
            reload(profile, server);
        } else {
            throw new IllegalArgumentException(String.format("The provided parameters (profile=%s, server=%s) " +
                    "are not correct. Please add profile", profile, server));
        }
    }

    private void reloadAll(String profile) {
        Predicate<String> profilePredicate = key -> (StringUtils.isBlank(profile) || key.equals(profile));
        profileReloaderMap.forEach((profileName, reloader) -> {
            if (profilePredicate.test(profileName)) {
                // TODO: Terminate all active queries before reload (ADBDEV-4987)
                reloader.reloadAll();
                log.info("Reload profile '{}' for all servers with reloader {}", profileName, reloader);
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
            // TODO: Terminate all active queries before reload (ADBDEV-4987)
            reloader.reload(server);
            log.info("Reload profile '{}' for server '{}' with reloader {}", profile, server, reloader);
        } else {
            String message = String.format(
                    "Profile '%s' doesn't support reloading methods. Skipping reloading for server '%s'. " +
                            "Profiles with supporting reloading methods: %s", profile, server, profileReloaderMap.keySet());
            log.error(message);
            throw new PxfRuntimeException(message);
        }
    }
}
