package org.greenplum.pxf.service.profile;

import org.greenplum.pxf.service.rest.dto.ProfileReloadRequestDto;
import org.springframework.stereotype.Service;

@Service
public class ProfileReloadServiceImpl implements ProfileReloadService {
    @Override
    public void reloadProfile(ProfileReloadRequestDto reloadRequestDto) {
        // Will be implemented as a part of the ADBDEV-4986 task
    }
}
