package org.greenplum.pxf.service.profile;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.greenplum.pxf.api.error.PxfRuntimeException;
import org.greenplum.pxf.api.model.Plugin;
import org.greenplum.pxf.service.rest.dto.ProfileReloadRequestDto;
import org.greenplum.pxf.service.utilities.BasePluginFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProfileReloadServiceImpl implements ProfileReloadService {
    private final static String ACCESSOR_KEY = "ACCESSOR";
    private final ProfilesConf profileConf;
    private final BasePluginFactory basePluginFactory;

    @Override
    public void reloadProfile(ProfileReloadRequestDto reloadRequestDto) {
        String profile = reloadRequestDto.getProfile();
        String server = reloadRequestDto.getServer();
        log.info("Received a request to reload a profile with the parameters: profile={}, server={}", profile, server);

        if (StringUtils.isBlank(profile)) {
            if (StringUtils.isBlank(server)) {
                // TODO: Terminate all active queries before reload (ADBDEV-4987)
                profileConf.getProfilesMap().keySet()
                        .forEach(this::reloadAll);
            } else {
                throw new IllegalArgumentException(String.format("The provided parameters (profile=%s, server=%s) " +
                        "are not correct. Please add profile", profile, server));
            }
        } else if (StringUtils.isBlank(server)) {
            // TODO: Terminate all active queries before reload (ADBDEV-4987)
            reloadAll(profile);
        } else {
            // TODO: Terminate all active queries before reload (ADBDEV-4987)
            reload(profile, server);
        }
    }

    private void reloadAll(String profile) {
        Optional<Plugin> accessor = getAccessor(profile);
        if (accessor.isPresent()) {
            log.info("Reload profile '{}' for all servers with accessor {}", profile, accessor.get());
            accessor.get().reloadAll();
        } else {
            log.info("Skipping reloading profile '{}' for all servers", profile);
        }
    }

    private void reload(String profile, String server) {
        Optional<Plugin> accessor = getAccessor(profile);
        if (accessor.isPresent()) {
            log.info("Reload profile '{}' for server '{}' with accessor {}", profile, server, accessor.get());
            accessor.get().reload(server);
        } else {
            log.info("Skipping reloading profile '{}' for server '{}'", profile, server);
        }
    }

    private Optional<Plugin> getAccessor(String profile) {
        try {
            Map<String, String> pluginMap = profileConf.getPlugins(profile);
            return Optional.ofNullable(pluginMap)
                    .map(plugins -> getAccessorInstance(plugins.get(ACCESSOR_KEY)));
        } catch (Exception e) {
            String message = String.format("Failed to get plugin of the profile '%s'. %s", profile, e.getMessage());
            log.error(message);
            throw new PxfRuntimeException(message, e);
        }
    }

    private Plugin getAccessorInstance(String accessorClassName) {
        try {
            Plugin instance = basePluginFactory.getPluginInstance(accessorClassName);
            log.debug("Initialize instance {} of the accessor class {}", instance, accessorClassName);
            return instance;
        } catch (Exception e) {
            log.error("Failed to initialize instance of the accessor class {}. {}", accessorClassName, e.getMessage());
            return null;
        }
    }
}
