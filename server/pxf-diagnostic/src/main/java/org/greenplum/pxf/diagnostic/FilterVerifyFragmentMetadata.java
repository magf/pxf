package org.greenplum.pxf.diagnostic;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class FilterVerifyFragmentMetadata implements FragmentMetadata {
    private String filter;
}
