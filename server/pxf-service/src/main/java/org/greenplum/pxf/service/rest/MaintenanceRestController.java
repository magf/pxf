package org.greenplum.pxf.service.rest;

import org.greenplum.pxf.service.profile.ProfileReloadService;
import org.greenplum.pxf.service.rest.dto.ProfileReloadRequestDto;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/pxf")
public class MaintenanceRestController {
    private final ProfileReloadService profileReloadService;

    public MaintenanceRestController(final ProfileReloadService profileReloadService) {
        this.profileReloadService = profileReloadService;
    }

    @PostMapping(value = "/reload")
    public void reload(@RequestBody ProfileReloadRequestDto reloadRequestDto) {
        profileReloadService.reloadProfile(reloadRequestDto);
    }
}
