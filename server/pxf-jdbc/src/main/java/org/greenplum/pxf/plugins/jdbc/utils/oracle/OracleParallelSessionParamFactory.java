package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Strings;

import java.util.HashMap;

public class OracleParallelSessionParamFactory {
    private static final String ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER = "\\.";

    public OracleParallelSessionParam create(String property, String value) {
        validateValue(property, value);

        HashMap<String, String> map = getParallelSessionParam(value);
        String clause = map.get("clause").toUpperCase();
        String statementType = map.get("statement_type").toUpperCase();
        String degreeOfParallelism = map.get("degree_of_parallelism");

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(getClause(clause, property));
        param.setStatementType(getStatementType(statementType, property));
        param.setDegreeOfParallelism(getDegreeOfParallelism(degreeOfParallelism, property));
        return param;
    }

    private void validateValue(String property, String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException(String.format(
                    "The parameter '%s' is empty in jdbc-site.xml", property)
            );
        }
        if (value.split(ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER).length < 2
                || value.split(ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER).length > 3) {
            throw new IllegalArgumentException(String.format(
                    "The parameter '%s' in jdbc-site.xml has to contain at least 2 but not more then 3 values delimited by %s",
                    property, ORACLE_JDBC_SESSION_PARALLEL_PROPERTY_DELIMITER)
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
        }
        return params;
    }

    private OracleParallelSessionParam.Clause getClause(String clause, String property) {
        try {
            return OracleParallelSessionParam.Clause.valueOf(clause);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                    "The 'clause' value '%s' in the parameter '%s' is not valid", clause, property)
            );
        }
    }

    private OracleParallelSessionParam.StatementType getStatementType(String statementType, String property) {
        try {
            return OracleParallelSessionParam.StatementType.valueOf(statementType);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(
                    "The 'statement type' value '%s' in the parameter '%s' is not valid", statementType, property)
            );
        }
    }

    private String getDegreeOfParallelism(String degreeOfParallelism, String property) {
        if (degreeOfParallelism == null) {
            return Strings.EMPTY;
        }
        try {
            Integer.parseInt(degreeOfParallelism);
            return degreeOfParallelism;
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException(String.format(
                    "The 'degree of parallelism' value '%s' in the parameter '%s' is not valid", degreeOfParallelism, property)
            );
        }
    }
}
