package org.greenplum.pxf.automation.structures.tables.pxf;

import io.qameta.allure.Step;
import org.greenplum.pxf.automation.structures.tables.basic.Table;
import org.greenplum.pxf.automation.utils.system.ProtocolEnum;
import org.greenplum.pxf.automation.utils.system.ProtocolUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Represent GPDB -> PXF external table.
 */
public abstract class ExternalTable extends Table {

    private String host = "127.0.0.1";

    private String port = "5888";

    private String path;

    private String fragmenter;

    private String accessor;

    private String resolver;

    private String dataSchema;

    private String format;

    private String formatter;

    private final List<String> formatterOptions = new ArrayList<>();

    private String delimiter;

    private String escape;

    private String newLine;

    private String[] userParameters;

    private String server;

    private String profile;

    private String errorTable;

    private int segmentRejectLimit = 0;

    private String segmentRejectLimitType = "ROWS";

    private String encoding;

    private String externalDataSchema;

    private boolean formatterMixedCase = false; // whether to mangle FORMATTER word into mixed case for testing

    public ExternalTable(String name, String[] fields, String path,
                         String format) {
        super(name, fields);
        this.path = path;
        this.format = format;
        if (ProtocolUtils.getProtocol() != ProtocolEnum.HDFS) {
            this.setServer("server=" + ProtocolUtils.getProtocol().value());
        }
    }

    @Override
    public String constructDropStmt(boolean cascade) {

        StringBuilder sb = new StringBuilder();

        sb.append("DROP EXTERNAL TABLE IF EXISTS ").append(getFullName());
        if (cascade) {
            sb.append(" CASCADE");
        }

        return sb.toString();
    }

    @Override
    protected String createHeader() {
        return "CREATE EXTERNAL TABLE " + getFullName();
    }

    @Override
    protected String createLocation() {

        StringBuilder sb = new StringBuilder();

        sb.append(" LOCATION (E'");
        sb.append(getLocation());
        sb.append("')");

        return sb.toString();
    }

    public String getLocation() {

        StringBuilder sb = new StringBuilder("pxf://");
        // GPDB mode does not use host:port in location URL

        sb.append(getPath());
        sb.append("?");
        sb.append(getLocationParameters());

        return sb.toString();
    }

    /**
     * Generates location for create query
     *
     * @return location parameters
     */
    protected String getLocationParameters() {

        StringBuilder sb = new StringBuilder();

        if (getProfile() != null) {
            appendParameter(sb, "PROFILE=" + getProfile());
        }

        if (getFragmenter() != null) {
            appendParameter(sb, "FRAGMENTER=" + getFragmenter());
        }

        if (getAccessor() != null) {
            appendParameter(sb, "ACCESSOR=" + getAccessor());
        }

        if (getResolver() != null) {
            appendParameter(sb, "RESOLVER=" + getResolver());
        }

        if (getDataSchema() != null) {
            // even though the new option name is DATA_SCHEMA, we can still use DATA-SCHEMA for an external table
            // to test backward compatibility
            appendParameter(sb, "DATA-SCHEMA=" + getDataSchema());
        }

        if (getExternalDataSchema() != null) {
            appendParameter(sb, "SCHEMA=" + getExternalDataSchema());
        }

        String[] params = getUserParameters();

        if (params != null) {

            for (String param : params) {
                appendParameter(sb, param);
            }
        }

        if (getServer() != null) {
            appendParameter(sb, getServer());
        }

        return sb.toString();
    }

    /**
     * Appends location parameters to {@link StringBuilder}, append '&' between
     * parameters
     *
     * @param sBuilder  {@link StringBuilder} to collect parameters
     * @param parameter to add to {@link StringBuilder}
     */
    protected void appendParameter(StringBuilder sBuilder, String parameter) {

        // if not the first parameter, add '&'
        if (!sBuilder.toString().isEmpty()) {
            sBuilder.append("&");
        }

        sBuilder.append(parameter);
    }

    @Override
    public String constructCreateStmt() {
        String createStatement = "";

        createStatement += createHeader();
        createStatement += createFields();
        createStatement += createLocation();

        if (getFormat() != null) {
            createStatement += " FORMAT '" + getFormat() + "'";

        }

        if (getFormatter() != null) {
            String formatterOption = isFormatterMixedCase() ? "FoRmAtTeR" : "formatter";
            createStatement += String.format(" (%s='%s'", formatterOption, getFormatter());
            if (!formatterOptions.isEmpty()) {
                createStatement += ", ";
                createStatement += formatterOptions.stream().collect(Collectors.joining(", "));
            }
            createStatement += ")";
        }

        boolean hasDelimiterOrEscapeOrNewLine =
                getDelimiter() != null || getEscape() != null || getNewLine() != null;

        if (hasDelimiterOrEscapeOrNewLine) {
            createStatement += " (";
        }

        if (getDelimiter() != null) {

            // if Escape character, no need for "'"
            String parsedDelimiter = getDelimiter();
            if (!parsedDelimiter.startsWith("E")) {
                parsedDelimiter = "'" + parsedDelimiter + "'";
            }
            createStatement += " DELIMITER " + parsedDelimiter ;
        }

        if (getEscape() != null) {

            // if Escape character, no need for "'"
            String parsedEscapeCharacter = getEscape();
            if (!parsedEscapeCharacter.startsWith("E")) {
                parsedEscapeCharacter = "'" + parsedEscapeCharacter + "'";
            }
            createStatement += " ESCAPE " + parsedEscapeCharacter;
        }

        if (getNewLine() != null) {

            String newLineCharacter = getNewLine();
            createStatement += " NEWLINE '" + newLineCharacter + "'";
        }

        if (hasDelimiterOrEscapeOrNewLine) {
            createStatement += ")";
        }

        if (getEncoding() != null) {
            createStatement += " ENCODING '" + getEncoding() + "'";
        }

        if (getErrorTable() != null) {
            createStatement += " LOG ERRORS";
        }

        if (getSegmentRejectLimit() > 0) {
            createStatement += " SEGMENT REJECT LIMIT "
                    + getSegmentRejectLimit() + " "
                    + getSegmentRejectLimitType();
        }

        createStatement += distribution();
        return createStatement;
    }

    public String getHost() {
        return host;
    }

    @Step("Set host")
    public void setHost(String host) {
        this.host = host;
    }

    public String getEncoding() {
        return encoding;
    }

    @Step("Set encoding")
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public String getEscape() {
        return escape;
    }

    @Step("Set delimiter")
    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    @Step("Set escape")
    public void setEscape(String escape) {
        this.escape = escape;
    }

    @Step("Set new line")
    public void setNewLine(String newLine) {
        this.newLine = newLine;
    }

    public String getNewLine() {
        return newLine;
    }

    public String getProfile() {
        return profile;
    }

    @Step("Set profile")
    public void setProfile(String profile) {
        this.profile = profile;
    }

    public String getErrorTable() {
        return errorTable;
    }

    @Step("Set error table")
    public void setErrorTable(String errorTable) {
        this.errorTable = errorTable;
    }

    public int getSegmentRejectLimit() {
        return segmentRejectLimit;
    }

    @Step("Set segment reject limit")
    public void setSegmentRejectLimit(int segmentRejectLimit) {
        this.segmentRejectLimit = segmentRejectLimit;
    }

    public String getSegmentRejectLimitType() {
        return segmentRejectLimitType;
    }

    public void setSegmentRejectLimitType(String segmentRejectLimitType) {
        this.segmentRejectLimitType = segmentRejectLimitType;
    }

    public void setSegmentRejectLimitAndType(int segmentRejectLimit,
                                             String segmentRejectLimitType) {
        this.segmentRejectLimit = segmentRejectLimit;
        this.segmentRejectLimitType = segmentRejectLimitType;
    }

    public String getFragmenter() {
        return fragmenter;
    }

    @Step("Set fragmenter")
    public void setFragmenter(String fragmenter) {
        this.fragmenter = fragmenter;
    }

    public String getAccessor() {
        return accessor;
    }

    @Step("Set accessor")
    public void setAccessor(String accessor) {
        this.accessor = accessor;
    }

    public String getResolver() {
        return resolver;
    }

    @Step("Set resolver")
    public void setResolver(String resolver) {
        this.resolver = resolver;
    }

    public String getDataSchema() {
        return dataSchema;
    }

    @Step("Set schema")
    public void setDataSchema(String dataSchema) {
        this.dataSchema = dataSchema;
    }

    public String getFormat() {
        return format;
    }

    @Step("Set format")
    public void setFormat(String format) {
        this.format = format;
    }

    public String getFormatter() {
        return formatter;
    }

    @Step("Set formatter")
    public void setFormatter(String formatter) {
        this.formatter = formatter;
    }

    public String getPort() {
        return port;
    }

    @Step("Set port")
    public void setPort(String port) {
        this.port = port;
    }

    public String getPath() {
        return path;
    }

    @Step("Set path")
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * Array of user parameters, each param in the format "KEY=VALUE"
     *
     * @param userParameters - array of user parameters
     */
    @Step("Set user parameters")
    public void setUserParameters(String[] userParameters) {

        this.userParameters = null;

        if (userParameters != null) {
            this.userParameters = Arrays.copyOf(userParameters,
                    userParameters.length);
        }
    }

    @Step("Add user parameter")
    public void addUserParameter(String userParameter) {
        if (userParameters == null) {
            userParameters = new String[] {userParameter};
        } else {
            userParameters = Arrays.copyOf(userParameters, userParameters.length + 1);
            userParameters[userParameters.length - 1] = userParameter;
        }
    }

    protected String[] getUserParameters() {
        return userParameters;
    }

    @Step("Set server")
    public void setServer(String server) {
        this.server = server;
    }

    public String getServer() {
        return server;
    }

    public String getExternalDataSchema() {
        return externalDataSchema;
    }

    @Step("Set external data schema")
    public void setExternalDataSchema(String externalDataSchema) {
        this.externalDataSchema = externalDataSchema;
    }

    @Step("Add formatter option")
    public void addFormatterOption(String formatterOption) {
        this.formatterOptions.add(formatterOption);
    }

    @Step("Set formatter options")
    public void setFormatterOptions(String[] formatterOptions) {
        for (String option : formatterOptions) {
            addFormatterOption(option);
        }
    }

    @Step("Set formatter mixed case")
    public boolean isFormatterMixedCase() {
        return formatterMixedCase;
    }

    public void setFormatterMixedCase(boolean formatterMixedCase) {
        this.formatterMixedCase = formatterMixedCase;
    }
}
