package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.Arrays;
import java.util.HashMap;

public class OracleParallelSessionParamFactory {
    private static final String ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER = "\\.";

    public OracleParallelSessionParam create(String property, String value) {
        validateValue(property, value);

        HashMap<String, String> map = getParallelSessionParam(value);
        String clause = map.get("clause").toUpperCase();
        String statementType = map.get("statement_type").toUpperCase();
        String degreeOfParallelism = map.get("degree_of_parallelism");

        validateParams(clause, statementType, degreeOfParallelism, property);

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(OracleParallelSessionParam.Clause.valueOf(clause));
        param.setStatementType(OracleParallelSessionParam.StatementType.valueOf(statementType));
        param.setDegreeOfParallelism(degreeOfParallelism);
        return param;
    }

    private void validateValue(String property, String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(String.format(
                    "Parameter %s is empty in jdbc-site.xml", property)
            );
        }
        if (value.split(ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER).length < 2
                || value.split(ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER).length > 3) {
            throw new IllegalArgumentException(String.format(
                    "Parameter %s in jdbc-site.xml has to contain at least 2 but not more then 3 values delimited by %s",
                    property, ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER)
            );
        }
    }

    private void validateParams(String clause, String statementType, String degreeOfParallelism, String property) {
        if (!isClauseValid(clause)) {
            throw new IllegalArgumentException(String.format(
                    "The 'clause' value %s in the parameter %s is not valid", clause, property)
            );
        }
        if (!isStatementTypeValid(statementType)) {
            throw new IllegalArgumentException(String.format(
                    "The 'statement type' value %s in the parameter %s is not valid", statementType, property)
            );
        }
        if (!isDegreeOfParallelismValid(degreeOfParallelism)) {
            throw new IllegalArgumentException(String.format(
                    "The 'degree of parallelism' value %s in the parameter %s is not valid", degreeOfParallelism, property)
            );
        }
    }

    private HashMap<String, String> getParallelSessionParam(String value) {
        HashMap<String, String> params = new HashMap<>();
        String[] values = value.split(ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER);
        params.put("clause", values[0]);
        params.put("statement_type", values[1]);
        if (values.length == 3 && Strings.isNotBlank(values[2])) {
            params.put("degree_of_parallelism", values[2]);
        } else {
            params.put("degree_of_parallelism", "");
        }
        return params;
    }

    private boolean isClauseValid(String value) {
        return Arrays.stream(OracleParallelSessionParam.Clause.values()).anyMatch(e -> e.name().equalsIgnoreCase(value));
    }

    private boolean isStatementTypeValid(String value) {
        return Arrays.stream(OracleParallelSessionParam.StatementType.values()).anyMatch(e -> e.name().equalsIgnoreCase(value));
    }

    private boolean isDegreeOfParallelismValid(String value) {
        if (Strings.isNotEmpty(value)) {
            try {
                Integer.parseInt(value);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }
}
