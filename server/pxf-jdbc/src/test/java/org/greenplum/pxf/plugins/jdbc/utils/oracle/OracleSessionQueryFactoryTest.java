package org.greenplum.pxf.plugins.jdbc.utils.oracle;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class OracleSessionQueryFactoryTest {

    @Test
    @SuppressWarnings("try")
    void createParallelSessionQueryWithForceAndDegreeOfParallelism() {
        String property = "jdbc.session.property.alter_session_parallel.1";
        String value = "force.query.4";
        String expectedResult = "ALTER SESSION FORCE PARALLEL QUERY PARALLEL 4";

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(OracleParallelSessionParam.Clause.FORCE);
        param.setStatementType(OracleParallelSessionParam.StatementType.QUERY);
        param.setDegreeOfParallelism("4");

        try (MockedConstruction<OracleParallelSessionParamFactory> mocked = mockConstruction(OracleParallelSessionParamFactory.class,
                (mock, context) -> when(mock.create(property, value)).thenReturn(param))) {
            OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
            String result = oracleSessionQueryFactory.create(property, value);
            assertEquals(expectedResult, result);
        }
    }

    @Test
    @SuppressWarnings("try")
    void createParallelSessionQueryWithForce() {
        String property = "jdbc.session.property.alter_session_parallel.1";
        String value = "force.dml";
        String expectedResult = "ALTER SESSION FORCE PARALLEL DML";

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(OracleParallelSessionParam.Clause.FORCE);
        param.setStatementType(OracleParallelSessionParam.StatementType.DML);
        param.setDegreeOfParallelism("");

        try (MockedConstruction<OracleParallelSessionParamFactory> mocked = mockConstruction(OracleParallelSessionParamFactory.class,
                (mock, context) -> when(mock.create(property, value)).thenReturn(param))) {
            OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
            String result = oracleSessionQueryFactory.create(property, value);
            assertEquals(expectedResult, result);
        }
    }

    @Test
    @SuppressWarnings("try")
    void createParallelSessionQueryWithEnable() {
        String property = "jdbc.session.property.alter_session_parallel.1";
        String value = "enable.dml.2";
        String expectedResult = "ALTER SESSION ENABLE PARALLEL DDL";

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(OracleParallelSessionParam.Clause.ENABLE);
        param.setStatementType(OracleParallelSessionParam.StatementType.DDL);
        param.setDegreeOfParallelism("2");

        try (MockedConstruction<OracleParallelSessionParamFactory> mocked = mockConstruction(OracleParallelSessionParamFactory.class,
                (mock, context) -> when(mock.create(property, value)).thenReturn(param))) {
            OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
            String result = oracleSessionQueryFactory.create(property, value);
            assertEquals(expectedResult, result);
        }
    }

    @Test
    @SuppressWarnings("try")
    void createParallelSessionQueryWithDisable() {
        String property = "jdbc.session.property.alter_session_parallel.1";
        String value = "disable.dml";
        String expectedResult = "ALTER SESSION DISABLE PARALLEL DML";

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(OracleParallelSessionParam.Clause.DISABLE);
        param.setStatementType(OracleParallelSessionParam.StatementType.DML);
        param.setDegreeOfParallelism("");

        try (MockedConstruction<OracleParallelSessionParamFactory> mocked = mockConstruction(OracleParallelSessionParamFactory.class,
                (mock, context) -> when(mock.create(property, value)).thenReturn(param))) {
            OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
            String result = oracleSessionQueryFactory.create(property, value);
            assertEquals(expectedResult, result);
        }
    }

    @Test
    @SuppressWarnings("try")
    void createNotParallelSessionQuery() {
        String property = "STATISTICS_LEVEL";
        String value = "TYPICAL";
        String expectedResult = "ALTER SESSION SET STATISTICS_LEVEL = TYPICAL";

        OracleParallelSessionParam param = new OracleParallelSessionParam();
        param.setClause(OracleParallelSessionParam.Clause.ENABLE);
        param.setStatementType(OracleParallelSessionParam.StatementType.DDL);
        param.setDegreeOfParallelism("2");

        try (MockedConstruction<OracleParallelSessionParamFactory> mocked = mockConstruction(OracleParallelSessionParamFactory.class,
                (mock, context) -> when(mock.create(property, value)).thenReturn(param))) {
            OracleSessionQueryFactory oracleSessionQueryFactory = new OracleSessionQueryFactory();
            String result = oracleSessionQueryFactory.create(property, value);
            assertEquals(expectedResult, result);
        }
    }
}