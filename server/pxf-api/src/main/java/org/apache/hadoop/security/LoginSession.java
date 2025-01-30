package org.apache.hadoop.security;

import lombok.Getter;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import javax.security.auth.Subject;
import java.util.Objects;

/**
 * This class stores information about Kerberos login details for a given configuration server.
 * A subset of fields establishes session identity based on the server configuration.
 * Other fields are the result of the login action and do not establish identity.
 * <p>
 * This class has to be a member of <code>org.apache.hadoop.security</code> package as it needs access
 * to package-private <code>User</code> class.
 */
@Getter
public class LoginSession {

    // fields establishing session identity from configuration
    private final String configDirectory;
    private final String principalName;
    private final String keytabPath;
    private final long kerberosMinMillisBeforeRelogin;
    private final float kerberosTicketRenewWindow;
    private final UserGroupInformation loginUser;
    private final Subject subject;
    private final boolean hadoopLoginContextEnable;
    private User user;

    /**
     * Creates a new session object with a config directory
     *
     * @param configDirectory server configuration directory
     */
    public LoginSession(String configDirectory) {
        this(configDirectory, null, null, null, null, 0L, 0f);
    }

    /**
     * Creates a new session object with a config directory and a login user
     *
     * @param configDirectory server configuration directory
     * @param loginUser       UserGroupInformation for the given principal after login to Kerberos was performed
     */
    public LoginSession(String configDirectory, UserGroupInformation loginUser) {
        this(configDirectory, null, null, loginUser, null, 0L, 0f);
    }

    /**
     * Creates a new session object.
     *
     * @param configDirectory           server configuration directory
     * @param principalName             Kerberos principal name to use to obtain tokens
     * @param keytabPath                full path to a keytab file for the principal
     * @param kerberosTicketRenewWindow the percentage of the ticket lifespan
     */
    public LoginSession(String configDirectory, String principalName, String keytabPath, long kerberosMinMillisBeforeRelogin, float kerberosTicketRenewWindow) {
        this(configDirectory, principalName, keytabPath, null, null, kerberosMinMillisBeforeRelogin, kerberosTicketRenewWindow);
    }

    /**
     * Creates a new session object.
     *
     * @param configDirectory                server configuration directory
     * @param principalName                  Kerberos principal name to use to obtain tokens
     * @param keytabPath                     full path to a keytab file for the principal
     * @param loginUser                      UserGroupInformation for the given principal after login to Kerberos was performed
     * @param subject                        the subject
     * @param kerberosMinMillisBeforeRelogin the number of milliseconds before re-login
     * @param kerberosTicketRenewWindow      the percentage of the ticket lifespan
     */
    public LoginSession(String configDirectory, String principalName, String keytabPath, UserGroupInformation loginUser,
                        Subject subject, long kerberosMinMillisBeforeRelogin, float kerberosTicketRenewWindow) {
        this.configDirectory = configDirectory;
        this.principalName = principalName;
        this.keytabPath = keytabPath;
        this.loginUser = loginUser;
        this.subject = subject;
        if (subject != null) {
            this.user = subject.getPrincipals(User.class).iterator().next();
        }
        this.kerberosMinMillisBeforeRelogin = kerberosMinMillisBeforeRelogin;
        this.kerberosTicketRenewWindow = kerberosTicketRenewWindow;
        this.hadoopLoginContextEnable = isHadoopLoginContextEnabled(loginUser);
    }

    /**
     * Check if the loginUser was created with HadoopLoginContext
     * The method UserGroupInformation#isFromKeytab() checks under the hood
     * if the loginUser is instance  of the HadoopLoginContext class.
     * @param loginUser   the login user
     * @return true if the loginUser was created with HadoopLoginContext
     */
    private boolean isHadoopLoginContextEnabled(UserGroupInformation loginUser) {
        if (Objects.isNull(loginUser)) {
            return false;
        }
        return loginUser.isFromKeytab();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoginSession that = (LoginSession) o;
        // ugi and subject are not included into expression below as they are transient derived values
        return Objects.equals(configDirectory, that.configDirectory) &&
                Objects.equals(principalName, that.principalName) &&
                Objects.equals(keytabPath, that.keytabPath) &&
                kerberosMinMillisBeforeRelogin == that.kerberosMinMillisBeforeRelogin &&
                kerberosTicketRenewWindow == that.kerberosTicketRenewWindow;
    }

    @Override
    public int hashCode() {
        return Objects.hash(configDirectory, principalName, keytabPath, kerberosMinMillisBeforeRelogin, kerberosTicketRenewWindow);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("config", configDirectory)
                .append("principal", principalName)
                .append("keytab", keytabPath)
                .append("kerberosMinMillisBeforeRelogin", kerberosMinMillisBeforeRelogin)
                .append("kerberosTicketRenewWindow", kerberosTicketRenewWindow)
                .toString();
    }
}
