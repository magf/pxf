package org.greenplum.pxf.service.profile;

import lombok.Getter;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * Profiles is the root element for the list of profiles
 * defined in the profiles XML file
 */
@Getter
@XmlRootElement(name = "profiles")
@XmlAccessorType(XmlAccessType.FIELD)
public class Profiles {
    @XmlElement(name = "profile")
    private List<Profile> profiles;

}
