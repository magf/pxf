package org.greenplum.pxf.plugins.jdbc.utils.oracle;

public class OracleParallelSessionParam {
    private Clause clause;
    private StatementType statementType;
    private String degreeOfParallelism;

    public void setClause(Clause clause) {
        this.clause = clause;
    }

    public void setStatementType(StatementType statementType) {
        this.statementType = statementType;
    }

    public void setDegreeOfParallelism(String degreeOfParallelism) {
        this.degreeOfParallelism = degreeOfParallelism;
    }

    public Clause getClause() {
        return clause;
    }

    public StatementType getStatementType() {
        return statementType;
    }

    public String getDegreeOfParallelism() {
        return degreeOfParallelism;
    }

    public enum Clause {
        ENABLE("ENABLE"),
        DISABLE("DISABLE"),
        FORCE("FORCE");

        public final String value;

        Clause(String value) {
            this.value = value;
        }
    }

    public enum StatementType {
        DML("DML"),
        DDL("DDL"),
        QUERY("QUERY");

        public final String value;

        StatementType(String value) {
            this.value = value;
        }
    }
}
