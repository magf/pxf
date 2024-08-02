package org.greenplum.pxf.api.examples;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.greenplum.pxf.api.utilities.FragmentMetadata;

@Setter
@Getter
@NoArgsConstructor
public class DemoFragmentMetadata implements FragmentMetadata {

    private String path;

    public DemoFragmentMetadata(String path) {
        this.path = path;
    }
}
