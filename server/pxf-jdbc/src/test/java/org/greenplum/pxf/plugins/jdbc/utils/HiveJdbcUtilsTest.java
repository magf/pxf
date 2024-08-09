package org.greenplum.pxf.plugins.jdbc.utils;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HiveJdbcUtilsTest {

    @Test
    public void testURLWithoutProperties() {
        String url = "jdbc:hive2://server:10000/default";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithoutImpersonationProperty() {
        String url = "jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf";
        assertEquals("jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithoutImpersonationPropertyAndWithQuestionMark() {
        String url = "jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf?hive.property=value";
        assertEquals("jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf;hive.server2.proxy.user=foo?hive.property=value",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithoutImpersonationPropertyAndWithHash() {
        String url = "jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf#hive.property=value";
        assertEquals("jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf;hive.server2.proxy.user=foo#hive.property=value",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithoutImpersonationPropertyWithQuestionMarkAndWithHash() {
        String url = "jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf?hive.question=value#hive.hash=value";
        assertEquals("jdbc:hive2://server:10000/default;foo=bar;saslQop=auth-conf;hive.server2.proxy.user=foo?hive.question=value#hive.hash=value",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyStart() {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar;otherProperty=otherValue;saslQop=auth-conf";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo;otherProperty=otherValue;saslQop=auth-conf",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyMiddle() {
        String url = "jdbc:hive2://server:10000/default;otherProperty=otherValue;hive.server2.proxy.user=bar;saslQop=auth-conf";
        assertEquals("jdbc:hive2://server:10000/default;otherProperty=otherValue;hive.server2.proxy.user=foo;saslQop=auth-conf",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyEnd() {
        String url = "jdbc:hive2://server:10000/default;otherProperty=otherValue;saslQop=auth-conf;hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;otherProperty=otherValue;saslQop=auth-conf;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyOnly() {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyAndQuestionMark() {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar?hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo?hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyAndQuestionMark2() {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar;other-property=value?hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo;other-property=value?hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyAndHash() {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar#hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo#hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithImpersonationPropertyAndQuestionMarkAndHash() {
        String url = "jdbc:hive2://server:10000/default;hive.server2.proxy.user=bar?hive.server2.proxy.user=bar#hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo?hive.server2.proxy.user=bar#hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testURLWithSimilarPropertyName() {
        String url = "jdbc:hive2://server:10000/default;non-hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;non-hive.server2.proxy.user=bar;hive.server2.proxy.user=foo",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testImpersonationPropertyAfterQuestionMark() {
        String url = "jdbc:hive2://server:10000/default?questionMarkProperty=questionMarkValue;hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo?questionMarkProperty=questionMarkValue;hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }

    @Test
    public void testImpersonationPropertyAfterHash() {
        String url = "jdbc:hive2://server:10000/default#questionMarkProperty=questionMarkValue;hive.server2.proxy.user=bar";
        assertEquals("jdbc:hive2://server:10000/default;hive.server2.proxy.user=foo#questionMarkProperty=questionMarkValue;hive.server2.proxy.user=bar",
                HiveJdbcUtils.updateImpersonationPropertyInHiveJdbcUrl(url, "foo"));
    }
}
