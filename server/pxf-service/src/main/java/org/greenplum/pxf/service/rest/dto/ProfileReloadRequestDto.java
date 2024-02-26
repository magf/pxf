package org.greenplum.pxf.service.rest.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class ProfileReloadRequestDto {
    private final String profile;
    private final String server;
}
