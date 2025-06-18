package org.greenplum.pxf.diagnostic;

import org.greenplum.pxf.api.filter.FilterParser;
import org.greenplum.pxf.api.model.BaseFragmenter;
import org.greenplum.pxf.api.model.Fragment;

import java.util.List;

/**
 * Test class for regression tests.
 * The only thing this class does is to take received filter string from
 * Greengage (FILTER) and return it in UserData back to Greengage for later
 * validation in Resolver/Accessor
 */
public class FilterVerifyFragmenter extends BaseFragmenter {

    /**
     * Returns one fragment with incoming filter string value as the user data.
     * If no incoming filter, then return "No Filter" as user data.
     *
     * @return one data fragment
     * @throws Exception when filter parsing fails
     */
    @Override
    public List<Fragment> getFragments() throws Exception {

        String filter = "No filter";

        // Validate the filterstring by parsing using a dummy filterBuilder
        if (context.hasFilter()) {
            filter = context.getFilterString();
            new FilterParser().parse(filter);
        }

        // Set filter value as returned user data.
        Fragment fragment = new Fragment("dummy_file_path",
                new FilterVerifyFragmentMetadata(filter));
        fragments.add(fragment);

        return fragments;
    }
}
