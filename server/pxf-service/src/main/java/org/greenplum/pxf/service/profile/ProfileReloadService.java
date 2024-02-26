package org.greenplum.pxf.service.profile;

import org.greenplum.pxf.service.rest.dto.ProfileReloadRequestDto;

public interface ProfileReloadService {

    void reloadProfile(ProfileReloadRequestDto reloadRequestDto);
}