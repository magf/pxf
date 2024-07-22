package org.greenplum.pxf.automation.structures.profiles;

import java.util.Objects;

/** Represents PXF Profile with all it's components. */
public class Profile {

	private String name;
	private String description = "";
	private String fragmenter = "";
	private String accessor = "";
	private String resolver = "";

	public Profile(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFragmenter() {
		return fragmenter;
	}

	public void setFragmenter(String fragmenter) {
		this.fragmenter = fragmenter;
	}

	public String getAccessor() {
		return accessor;
	}

	public void setAccessor(String accessor) {
		this.accessor = accessor;
	}

	public String getResolver() {
		return resolver;
	}

	public void setResolver(String resolver) {
		this.resolver = resolver;
	}

	@Override
	public String toString() {

		return "Profile: " + name + " (fragmenter=" + fragmenter + ", accessor=" + accessor + ", resolver=" + resolver + ")";
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Profile profile = (Profile) o;
		return Objects.equals(name, profile.name) &&
				Objects.equals(description, profile.description)
				&& Objects.equals(fragmenter, profile.fragmenter)
				&& Objects.equals(accessor, profile.accessor)
				&& Objects.equals(resolver, profile.resolver);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, description, fragmenter, accessor, resolver);
	}
}