package org.apache.hadoop.security;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class PxfUserGroupInformationTest {

    private String serverName;
    private Configuration configuration;
    private UserGroupInformation ugi;
    private Subject subject;
    private Subject subjectWithKerberosKeyTab;
    private User user;
    private LoginContext mockLoginContext, mockAnotherLoginContext;
    private PxfUserGroupInformation.LoginContextProvider mockLoginContextProvider;
    private KerberosTicket mockTGT;
    private final KerberosPrincipal tgtPrincipal = new KerberosPrincipal("krbtgt/EXAMPLE.COM@EXAMPLE.COM");
    private final KerberosPrincipal nonTgtPrincipal = new KerberosPrincipal("some/somewhere@EXAMPLE.COM");
    private LoginSession session;
    private long nowMs;
    private PxfUserGroupInformation pxfUserGroupInformation;

    private static final String PROPERTY_KEY_JAVA_VENDOR = "java.vendor";
    private static final String PROPERTY_KEY_KERBEROS_KDC = "java.security.krb5.kdc";
    private static final String PROPERTY_KEY_KERBEROS_REALM = "java.security.krb5.realm";
    private static String kdcDefault;
    private static String realmDefault;
    private static String javaVendor;

    @BeforeAll
    public static void setProperties() {
        // simulate presence of krb.conf file, important for prevention of test pollution when creating Users
        kdcDefault = System.setProperty(PROPERTY_KEY_KERBEROS_KDC, "localhost");
        realmDefault = System.setProperty(PROPERTY_KEY_KERBEROS_REALM, "DEFAULT_REALM");

        // Not IBM. Refer to org.apache.hadoop.security.authentication.util.KerberosUtil.getKrb5LoginModuleName
        javaVendor = System.setProperty(PROPERTY_KEY_JAVA_VENDOR, "foobar");
    }

    @AfterAll
    public static void resetProperties() {
        resetProperty(PROPERTY_KEY_JAVA_VENDOR, javaVendor);
        resetProperty(PROPERTY_KEY_KERBEROS_KDC, kdcDefault);
        resetProperty(PROPERTY_KEY_KERBEROS_REALM, realmDefault);
    }

    @BeforeEach
    public void setup() throws Exception {

        // prepare objects
        nowMs = System.currentTimeMillis();
        configuration = new Configuration();
        user = new User("user");
        serverName = "server";
        pxfUserGroupInformation = new PxfUserGroupInformation();

        // prepare common mocks
        mockTGT = mock(KerberosTicket.class);

        KeyTab mockKeyTab = mock(KeyTab.class);
        // subject will have a known User as principal and mock TGT credential, train it to have appropriate expiration
        subject = new Subject(false, Sets.newHashSet(user), Sets.newHashSet(), Sets.newHashSet(mockTGT));

        // subject with a Kerberos Keytab
        subjectWithKerberosKeyTab = new Subject(false, Sets.newHashSet(user), Sets.newHashSet(), Sets.newHashSet(mockTGT, mockKeyTab));

        // train to return mock Login Context when created with constructor
        mockLoginContext = mock(LoginContext.class);
        mockLoginContextProvider = mock(PxfUserGroupInformation.LoginContextProvider.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockLoginContext);

        pxfUserGroupInformation.loginContextProvider = mockLoginContextProvider;

        // setup PUGI to use a known subject instead of creating a brand new one
        pxfUserGroupInformation.subjectProvider = () -> subject;
        doNothing().when(mockLoginContext).login();
    }

    /* ---------- Tests with the context login process (pxf.service.kerberos.hadoop-login-context-enable = false) ---------- */

    @Test
    public void testLoginFromKeytabMinMillisFromConfig() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "33");
        ugi = new UserGroupInformation(subject);

        session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");

        // assert that the login session was created with properly wired up ugi/subject/user/loginContext
        assertEquals(33000, session.getKerberosMinMillisBeforeRelogin()); // will pick from configuration
        assertSessionInfo(session, mockLoginContext, ugi, subject, user);

        // verify that login() was called
        verify(mockLoginContext).login();
    }

    @Test
    public void testLoginFromKeytabMinMillisFromDefault() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        ugi = new UserGroupInformation(subject);

        session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");

        // assert that the login session was created with properly wired up ugi/subject/user/loginContext
        assertEquals(60000, session.getKerberosMinMillisBeforeRelogin()); // will pick from default
        assertSessionInfo(session, mockLoginContext, ugi, subject, user);

        // verify that login() was called
        verify(mockLoginContext).login();
    }

    @Test
    public void testLoginFromKeytabRenewWindowFromConfig() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        configuration.setFloat("pxf.service.kerberos.ticket-renew-window", 0.2f);
        ugi = new UserGroupInformation(subject);

        session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");

        // assert that the login session was created with properly wired up ugi/subject/user/loginContext
        assertEquals(0.2f, session.getKerberosTicketRenewWindow()); // will pick from configuration
        assertSessionInfo(session, mockLoginContext, ugi, subject, user);

        // verify that login() was called
        verify(mockLoginContext).login();
    }

    @Test
    public void testLoginFromKeytabRenewWindowDefault() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        ugi = new UserGroupInformation(subject);

        session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");

        // assert that the login session was created with properly wired up ugi/subject/user/loginContext
        assertEquals(0.8f, session.getKerberosTicketRenewWindow()); // will pick from default
        assertSessionInfo(session, mockLoginContext, ugi, subject, user);

        // verify that login() was called
        verify(mockLoginContext).login();
    }

    @Test
    public void testLoginFromKeytabRenewWindowInvalidValue() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        configuration.set("pxf.service.kerberos.ticket-renew-window", "1.2");
        ugi = new UserGroupInformation(subject);

        Exception e = assertThrows(IllegalArgumentException.class,
                () -> pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab"));
        assertEquals("Invalid value for pxf.service.kerberos.ticket-renew-window of 1.200000 for server server. Please choose a value between 0 and 1.", e.getMessage());

        // verify that login() was called
        verify(mockLoginContext).login();
    }

    /* ---------- Test below follow either cause no re-login (noop) or error out ---------- */

    @Test
    public void testReloginFromKeytabNoopForNonKerberos() throws KerberosAuthException {
        user.setLogin(mockLoginContext);
        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        // do NOT set authentication method of UGI to KERBEROS, will cause NOOP for relogin
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(false).when(session).isHadoopLoginContextEnable();

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verifyNoInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopForNonKeytab() throws KerberosAuthException {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        user.setLogin(mockLoginContext);
        ugi = new UserGroupInformation(subject);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subject, 1, 0.8f));
        doReturn(false).when(session).isHadoopLoginContextEnable();

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verifyNoInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopInsufficientTimeElapsed() throws KerberosAuthException {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        user.setLogin(mockLoginContext);
        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        user.setLastLogin(nowMs); // simulate just logged in
        // set 33 secs between re-login attempts
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 55000L, 0.8f));
        doReturn(false).when(session).isHadoopLoginContextEnable();

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verifyNoInteractions(mockLoginContext); // proves noop
    }

    @Test
    public void testReloginFromKeytabNoopTGTValidForLongTime() throws KerberosAuthException {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 1 hr from now, we are at 50% of renew window
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 3600 * 1000L));

        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);
        // leave user.lastLogin at 0 to simulate old login
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(false).when(session).isHadoopLoginContextEnable();

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verifyNoInteractions(mockLoginContext);
    }

    @Test
    public void testReloginFromKeytabFailsNoLogin() {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        user.setLogin(null); // simulate missing login context for the user
        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(ugi.getAuthenticationMethod()).thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        when(ugi.isFromKeytab()).thenReturn(true);
        // leave user.lastLogin at 0 to simulate old login
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(ugi).when(session).getLoginUser();
        doReturn(false).when(session).isHadoopLoginContextEnable();

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session));
        assertEquals(" loginUserFromKeyTab must be done first", e.getMessage());
    }

    @Test
    public void testReloginFromKeytabFailsNoKeytab() {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        user.setLogin(mockLoginContext);
        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(ugi.getAuthenticationMethod()).thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        when(ugi.isFromKeytab()).thenReturn(true);
        // leave user.lastLogin at 0 to simulate old login
        session = spy(new LoginSession("config", "principal", null, ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(ugi).when(session).getLoginUser();
        doReturn(false).when(session).isHadoopLoginContextEnable();

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session));
        assertEquals(" loginUserFromKeyTab must be done first", e.getMessage());
    }

    /* ---------- Test below follow full login path via a few alternatives ---------- */

    @Test
    public void testReloginFromKeytabNoValidTGT() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");

        assertEquals(2, subjectWithKerberosKeyTab.getPrivateCredentials().size()); // subject has 2 tickets

        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(nonTgtPrincipal); // ticket is not from krbtgt, so not valid

        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(ugi.getAuthenticationMethod()).thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        when(ugi.isFromKeytab()).thenReturn(true);
        // leave user.lastLogin at 0 to simulate old login
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(ugi).when(session).getLoginUser();
        doReturn(false).when(session).isHadoopLoginContextEnable();

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = mock(LoginContext.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockAnotherLoginContext);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        assertNotSame(mockLoginContext, user.getLogin());
        assertSame(mockAnotherLoginContext, user.getLogin());
        assertTrue(user.getLastLogin() > 0); // login timestamp is updated

        /* subject's non-TGT ticket has been removed, in reality another one would be created by login process,
         * but we are not mocking it here.
         */
        assertEquals(1, subject.getPrivateCredentials().size());

        verify(mockLoginContext).logout();
        verify(mockAnotherLoginContext).login();
        verify(mockTGT).destroy(); // subject's non-TGT ticket has been destroyed
    }

    @Test
    public void testReloginFromKeytabValidTGTWillExpireSoon() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 10 mins, we are at 6/7 or 85% > 80% of renew window
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 600 * 1000L));

        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(ugi.getAuthenticationMethod()).thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        when(ugi.isFromKeytab()).thenReturn(true);
        // leave user.lastLogin at 0 to simulate old login
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(ugi).when(session).getLoginUser();
        doReturn(false).when(session).isHadoopLoginContextEnable();

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = mock(LoginContext.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockAnotherLoginContext);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        assertNotSame(mockLoginContext, user.getLogin());
        assertSame(mockAnotherLoginContext, user.getLogin());
        assertTrue(user.getLastLogin() > 0); // login timestamp is updated

        verify(mockLoginContext).logout();
        verify(mockAnotherLoginContext).login();
    }

    @Test
    public void testReloginFromKeytabDifferentRenewWindow() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");
        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 1 hr, we are at 6/12 or 50% which is less
        // than the default renew window of 80%
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 3600 * 1000L));

        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(ugi.getAuthenticationMethod()).thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        when(ugi.isFromKeytab()).thenReturn(true);
        // leave user.lastLogin at 0 to simulate old login
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(ugi).when(session).getLoginUser();
        doReturn(false).when(session).isHadoopLoginContextEnable();

        // with the default threshold, there should be no change to the login
        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        assertSame(mockLoginContext, user.getLogin());
        assertTrue(user.getLastLogin() == 0); // login timestamp is updated

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = mock(LoginContext.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockAnotherLoginContext);

        // change the renew threshold to 50%
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.5f));
        doReturn(false).when(session).isHadoopLoginContextEnable();
        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        assertNotSame(mockLoginContext, user.getLogin());
        assertSame(mockAnotherLoginContext, user.getLogin());
        assertTrue(user.getLastLogin() > 0); // login timestamp is updated

        verify(mockLoginContext).logout();
        verify(mockAnotherLoginContext).login();
    }

    @Test
    public void testReloginFromKeytabThrowsExceptionOnLoginFailure() throws Exception {
        configuration.set("pxf.service.kerberos.hadoop-login-context-enable", "false");

        user.setLogin(mockLoginContext);
        when(mockTGT.getServer()).thenReturn(nonTgtPrincipal); // ticket is not from krbtgt, so not valid
        UserGroupInformation ugi = mock(UserGroupInformation.class);
        when(ugi.getAuthenticationMethod()).thenReturn(UserGroupInformation.AuthenticationMethod.KERBEROS);
        when(ugi.isFromKeytab()).thenReturn(true);
        // leave user.lastLogin at 0 to simulate old login
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(ugi).when(session).getLoginUser();
        doReturn(false).when(session).isHadoopLoginContextEnable();

        // train to return another LoginContext when it is constructed during re-login
        mockAnotherLoginContext = mock(LoginContext.class);
        when(mockLoginContextProvider.newLoginContext(anyString(), any(), any())).thenReturn(mockAnotherLoginContext);
        doThrow(new LoginException("foo")).when(mockAnotherLoginContext).login(); // simulate login failure

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session));
        assertEquals("Login failure for principal: principal from keytab keytab javax.security.auth.login.LoginException: foo", e.getMessage());
    }

    /* ---------- Tests with the Hadoop context login process (pxf.service.kerberos.hadoop-login-context-enable = true) ---------- */
    @Test
    public void testHadoopContextLoginFromKeytabMinMillisFromConfigHadoopContext() throws Exception {
        configuration.set("hadoop.kerberos.min.seconds.before.relogin", "33");

        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);

        try (MockedStatic<UserGroupInformation> ugiMockedStatic = mockStatic(UserGroupInformation.class)) {
            ugiMockedStatic.when(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)))
                    .thenReturn(ugi);
            session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");
            // assert that the login session was created with properly wired up ugi/subject/user/loginContext
            assertEquals(33000, session.getKerberosMinMillisBeforeRelogin()); // will pick from configuration
            assertHadoopContextSessionInfo(session, ugi, subjectWithKerberosKeyTab, user);

            ugiMockedStatic.verify(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)));
        }
    }

    @Test
    public void testHadoopContextLoginFromKeytabMinMillisFromDefault() throws Exception {
        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);

        try (MockedStatic<UserGroupInformation> ugiMockedStatic = mockStatic(UserGroupInformation.class)) {
            ugiMockedStatic.when(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)))
                    .thenReturn(ugi);
            session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");
            // assert that the login session was created with properly wired up ugi/subject/user/loginContext
            assertEquals(60000, session.getKerberosMinMillisBeforeRelogin()); // will pick from configuration
            assertHadoopContextSessionInfo(session, ugi, subjectWithKerberosKeyTab, user);

            ugiMockedStatic.verify(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)));
        }
    }

    @Test
    public void testHadoopContextLoginFromKeytabRenewWindowFromConfig() throws Exception {
        configuration.setFloat("pxf.service.kerberos.ticket-renew-window", 0.2f);

        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);

        try (MockedStatic<UserGroupInformation> ugiMockedStatic = mockStatic(UserGroupInformation.class)) {
            ugiMockedStatic.when(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)))
                    .thenReturn(ugi);
            session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");
            // assert that the login session was created with properly wired up ugi/subject/user/loginContext
            assertEquals(0.2f, session.getKerberosTicketRenewWindow()); // will pick from configuration
            assertHadoopContextSessionInfo(session, ugi, subjectWithKerberosKeyTab, user);

            ugiMockedStatic.verify(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)));
        }
    }

    @Test
    public void testHadoopContextLoginFromKeytabRenewWindowDefault() throws Exception {
        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);

        try (MockedStatic<UserGroupInformation> ugiMockedStatic = mockStatic(UserGroupInformation.class)) {
            ugiMockedStatic.when(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)))
                    .thenReturn(ugi);
            session = pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab");
            // assert that the login session was created with properly wired up ugi/subject/user/loginContext
            assertEquals(0.8f, session.getKerberosTicketRenewWindow()); // will pick from configuration
            assertHadoopContextSessionInfo(session, ugi, subjectWithKerberosKeyTab, user);

            ugiMockedStatic.verify(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)));
        }
    }

    @Test
    public void testHadoopContextLoginFromKeytabRenewWindowInvalidValue() {
        configuration.set("pxf.service.kerberos.ticket-renew-window", "1.2");

        ugi = new UserGroupInformation(subjectWithKerberosKeyTab);
        ugi.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS);

        try (MockedStatic<UserGroupInformation> ugiMockedStatic = mockStatic(UserGroupInformation.class)) {
            ugiMockedStatic.when(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)))
                    .thenReturn(ugi);

            Exception e = assertThrows(IllegalArgumentException.class,
                    () -> pxfUserGroupInformation.loginUserFromKeytab(configuration, "server", "config-dir", "principal/some.host.com@EXAMPLE.COM", "/path/to/keytab"));
            assertEquals("Invalid value for pxf.service.kerberos.ticket-renew-window of 1.200000 for server server. Please choose a value between 0 and 1.", e.getMessage());

            ugiMockedStatic.verify(() -> UserGroupInformation.loginUserFromKeytabAndReturnUGI(any(String.class), any(String.class)));
        }
    }

    /* ---------- Test below follow either cause no re-login (noop) or error out ---------- */

    @Test
    public void testHadoopContextReloginFromKeytabNoopForNonKerberos() throws IOException {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getSubject()).thenReturn(subjectWithKerberosKeyTab);
        when(mockUgi.hasKerberosCredentials()).thenReturn(false);
        when(mockUgi.isFromKeytab()).thenReturn(true);
        // do NOT set authentication method of UGI to KERBEROS, will cause NOOP for relogin
        session = spy(new LoginSession("config", "principal", "keytab", mockUgi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(true).when(session).isHadoopLoginContextEnable();

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verify(mockUgi, times(0)).reloginFromKeytab();
    }

    @Test
    public void testHadoopContextReloginFromKeytabNoopForNonKeytab() throws IOException {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getSubject()).thenReturn(subjectWithKerberosKeyTab);
        when(mockUgi.isFromKeytab()).thenReturn(false);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true); // UserGroupInformation.AuthenticationMethod.KERBEROS;
        session = spy(new LoginSession("config", "principal", "keytab", mockUgi, subject, 1, 0.8f));
        doReturn(true).when(session).isHadoopLoginContextEnable();

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verify(mockUgi, times(0)).reloginFromKeytab(); // proves noop
    }

    @Test
    public void testHadoopContextReloginFromKeytabNoopInsufficientTimeElapsed() throws IOException {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getSubject()).thenReturn(subjectWithKerberosKeyTab);
        when(mockUgi.isFromKeytab()).thenReturn(true);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true); // UserGroupInformation.AuthenticationMethod.KERBEROS
        user.setLastLogin(nowMs);
        session = new LoginSession("config", "principal", "keytab", mockUgi, subjectWithKerberosKeyTab, 55000L, 0.8f);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verify(mockUgi, times(0)).reloginFromKeytab(); // proves noop
    }

    @Test
    public void testHadoopContextReloginFromKeytabNoopTGTValidForLongTime() throws IOException {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getSubject()).thenReturn(subjectWithKerberosKeyTab);
        when(mockUgi.isFromKeytab()).thenReturn(true);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true); // UserGroupInformation.AuthenticationMethod.KERBEROS

        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 1 hr from now, we are at 50% of renew window
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 3600 * 1000L));

        // leave user.lastLogin at 0 to simulate old login
        session = new LoginSession("config", "principal", "keytab", mockUgi, subjectWithKerberosKeyTab, 1, 0.8f);

        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verify(mockUgi, times(0)).reloginFromKeytab(); // proves noop
    }

    @Test
    public void testHadoopContextReloginFromKeytabFailsNoLogin() {
        UserGroupInformation ugi = null;
        session = spy(new LoginSession("config", "principal", "keytab", ugi, subjectWithKerberosKeyTab, 1, 0.8f));
        doReturn(true).when(session).isHadoopLoginContextEnable();

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session));
        assertEquals(" loginUserFromKeyTab must be done first", e.getMessage());
    }

    @Test
    public void testHadoopContextReloginFromKeytabFailsNoKeytab() {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getSubject()).thenReturn(subjectWithKerberosKeyTab);
        when(mockUgi.isFromKeytab()).thenReturn(true);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true); // UserGroupInformation.AuthenticationMethod.KERBEROS
        session = spy(new LoginSession("config", "principal", null, mockUgi, subjectWithKerberosKeyTab, 1, 0.8f));

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session));
        assertEquals(" loginUserFromKeyTab must be done first", e.getMessage());
    }

    /* ---------- Test below follow full login path via a few alternatives ---------- */

    @Test
    public void testHadoopContextReloginFromKeytabNoValidTGT() throws Exception {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getSubject()).thenReturn(subjectWithKerberosKeyTab);
        when(mockUgi.isFromKeytab()).thenReturn(true);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true); // UserGroupInformation.AuthenticationMethod.KERBEROS

        when(mockTGT.getServer()).thenReturn(null);

        session = new LoginSession("config", "principal", "keytab", mockUgi, subjectWithKerberosKeyTab, 1, 0.8f);
        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verify(mockUgi, times(1)).reloginFromKeytab();
    }

    @Test
    public void testHadoopContextReloginFromKeytabValidTGTWillExpireSoon() throws Exception {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getSubject()).thenReturn(subjectWithKerberosKeyTab);
        when(mockUgi.isFromKeytab()).thenReturn(true);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true);
        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 10 mins, we are at 6/7 or 85% > 80% of renew window
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 600 * 1000L));

        session = new LoginSession("config", "principal", "keytab", mockUgi, subjectWithKerberosKeyTab, 1, 0.8f);
        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verify(mockUgi, times(1)).reloginFromKeytab();
    }

    @Test
    public void testHadoopContextReloginFromKeytabDifferentRenewWindow() throws Exception {
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.getSubject()).thenReturn(subjectWithKerberosKeyTab);
        when(mockUgi.isFromKeytab()).thenReturn(true);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true);
        when(mockTGT.getServer()).thenReturn(tgtPrincipal);

        // TGT validity started 1 hr ago, valid for another 1 hr, we are at 6/12 or 50% which is less
        // than the default renew window of 80%
        when(mockTGT.getStartTime()).thenReturn(new Date(nowMs - 3600 * 1000L));
        when(mockTGT.getEndTime()).thenReturn(new Date(nowMs + 3600 * 1000L));

        session = new LoginSession("config", "principal", "keytab", mockUgi, subjectWithKerberosKeyTab, 1, 0.8f);

        // with the default threshold, there should be no change to the login
        verify(mockUgi, times(0)).reloginFromKeytab();

        // change the renew threshold to 50%
        session = new LoginSession("config", "principal", "keytab", mockUgi, subjectWithKerberosKeyTab, 1, 0.5f);
        pxfUserGroupInformation.reloginFromKeytab(serverName, session);

        verify(mockUgi, times(1)).reloginFromKeytab();
    }

    @Test
    public void testHadoopContextReloginFromKeytabThrowsExceptionOnLoginFailure() throws Exception {
        when(mockTGT.getServer()).thenReturn(nonTgtPrincipal);
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(mockUgi.hasKerberosCredentials()).thenReturn(true);
        when(mockUgi.isFromKeytab()).thenReturn(true);
        session = new LoginSession("config", "principal", "keytab", mockUgi, subjectWithKerberosKeyTab, 1, 0.8f);

        doThrow(new KerberosAuthException("foo")).when(mockUgi).reloginFromKeytab(); // simulate login failure

        Exception e = assertThrows(KerberosAuthException.class,
                () -> pxfUserGroupInformation.reloginFromKeytab(serverName, session));
        assertEquals("Login failure for principal: principal from keytab keytab org.apache.hadoop.security.KerberosAuthException:  foo", e.getMessage());
    }

    /* --------------------------------------------------------------------------------------------------------------------------- */

    private static void assertSessionInfo(LoginSession session, LoginContext loginContext, UserGroupInformation ugi, Subject subject, User user) {
        assertEquals("/path/to/keytab", session.getKeytabPath());
        assertEquals("principal/some.host.com@EXAMPLE.COM", session.getPrincipalName());
        assertEquals(ugi, session.getLoginUser()); // UGI equality only compares enclosed subjects
        assertNotSame(ugi, session.getLoginUser()); // UGI equality only compares enclosed subjects
        assertSame(subject, session.getSubject());
        assertSame(user, session.getUser());
        assertSame(loginContext, session.getUser().getLogin());
        assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS, session.getLoginUser().getAuthenticationMethod());
    }

    private static void assertHadoopContextSessionInfo(LoginSession session, UserGroupInformation ugi, Subject subject, User user) {
        assertEquals("/path/to/keytab", session.getKeytabPath());
        assertEquals("principal/some.host.com@EXAMPLE.COM", session.getPrincipalName());
        assertEquals(ugi, session.getLoginUser()); // UGI equality only compares enclosed subjects
        assertSame(subject, session.getSubject());
        assertSame(user, session.getUser());
        assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS, session.getLoginUser().getAuthenticationMethod());
    }

    private static void resetProperty(String key, String val) {
        if (val != null) {
            System.setProperty(key, val);
            return;
        }
        System.clearProperty(key);
    }
}
