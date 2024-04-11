package sqlancer.common.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.MainOptions;
import sqlancer.SQLGlobalState;
import sqlancer.StateLogger;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.transaction.TxStatement;
import sqlancer.tidb.transaction.TxStatementExecutionResult;
import sqlancer.tidb.transaction.TxTestExecutionResult;

public abstract class TxBase<S extends SQLGlobalState<?, ?>> implements TestOracle {

    protected final S state;
    protected final MainOptions options;
    protected final StateLogger logger;

    public TxBase(S state) {
        this.state = state;
        this.options = state.getOptions();
        this.logger = state.getLogger();
    }

    public void reproduceDatabase(List<Query<?>> dbInitQueries) throws SQLException {
        for (Query<?> query : dbInitQueries) {
            SQLQueryAdapter queryAdapter = (SQLQueryAdapter) query;
            // Maybe need to retry, like sqlancer
            queryAdapter.execute(state);
        }
        try {
            state.updateSchema();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public String compareAllResults(TxTestExecutionResult testResult, TxTestExecutionResult oracleResult) {
        List<TxStatementExecutionResult> stmtExecResults = testResult.getStatementExecutionResults();
        List<TxStatementExecutionResult> stmtOracleResults = oracleResult.getStatementExecutionResults();
        for (TxStatementExecutionResult stmtExecResult : stmtExecResults) {
            if (stmtExecResult.isBlocked()) { // We skip comparing the block state for now.
                continue;
            }
            TxStatementExecutionResult stmtOracleResult = null;
            for (TxStatementExecutionResult oResult : stmtOracleResults) {
                if (oResult.getStatement().equals(stmtExecResult.getStatement())) {
                    stmtOracleResult = oResult;
                    break;
                }
            }
            TxStatement stmt = stmtExecResult.getStatement();
            
            StringBuilder compareResult = new StringBuilder();
            if ((!stmtExecResult.reportError() && stmtOracleResult.reportError())
                    || (stmtExecResult.reportError() && !stmtOracleResult.reportError())) {
                if (stmtExecResult.reportDeadlock()) {
                    continue;
                }
                compareResult.append("Error: Inconsistent reporting error\n");
            } else if (stmtExecResult.reportError() && stmtOracleResult.reportError()
                    && !stmtExecResult.getErrorInfo().equals(stmtOracleResult.getErrorInfo())) {
                compareResult.append("Error: Inconsistent error info\n");
            }
            if (compareResult.length() > 0) {
                compareResult.append(stmt.toString());
                compareResult.append(stmtExecResult.getErrorInfo());
                compareResult.append(stmtOracleResult.getErrorInfo());
                return compareResult.toString();
            }
            
            if (stmtExecResult.reportWarning() && !stmtOracleResult.reportWarning()
                    || !stmtExecResult.reportWarning() && stmtOracleResult.reportWarning()) {
                compareResult.append("Error: Inconsistent reporting warning\n");
            } else if (stmtExecResult.reportWarning() && stmtOracleResult.reportWarning()
                    && !stmtExecResult.getWarningInfo().equals(stmtOracleResult.getWarningInfo())) {
                compareResult.append("Error: Inconsistent warning info\n");
            }
            if (compareResult.length() > 0) {
                compareResult.append(stmt.toString());
                compareResult.append(stmtExecResult.getWarningInfo().toString());
                compareResult.append(stmtOracleResult.getWarningInfo().toString());
                return compareResult.toString();
            }
            
            if (stmt.getType() == TxStatement.StatementType.SELECT
                    || stmt.getType() == TxStatement.StatementType.SELECT_FOR_UPDATE) {
                String selectCompareResult = compareQueryResult(stmtExecResult.getResult(),
                        stmtOracleResult.getResult());
                if (!selectCompareResult.isEmpty()) {
                    compareResult.append("Error: Inconsistent query result\n");
                    compareResult.append(stmt.toString());
                    compareResult.append(selectCompareResult);
                    return compareResult.toString();
                }
            }
        }

        return compareFinalDBState(testResult, oracleResult);
    }

    public String compareFinalDBState(TxTestExecutionResult testResult, TxTestExecutionResult oracleResult) {
        for (Map.Entry<String, List<Object>> finalState : testResult.getDbFinalStates().entrySet()) {
            List<Object> execFinalState = finalState.getValue();
            List<Object> oracleFinalState = oracleResult.getDbFinalStates().get(finalState.getKey());
            String compareResultInfo = compareQueryResult(execFinalState, oracleFinalState);
            if (!compareResultInfo.isEmpty()) {
                return "Error: Inconsistent final database state\n" + compareResultInfo;
            }
        }
        return "";
    }

    private String compareQueryResult(List<Object> queryResult1, List<Object> queryResult2) {
        if (queryResult1 == null && queryResult2 == null) {
            return "";
        } else if (queryResult1 == null || queryResult2 == null) {
            return "Error: One query result is NULL";
        }
        if (queryResult1.size() != queryResult2.size()) {
            return "Error: The size of query results is different";
        }
        List<String> qRes1 = preprocessQueryResult(queryResult1);
        List<String> qRes2 = preprocessQueryResult(queryResult2);
        for (int i = 0; i < qRes1.size(); i++) {
            String r1 = qRes1.get(i);
            String r2 = qRes2.get(i);
            if (!r1.equals(r2)) {
                return "Error: (" + i + ")th values is different [" + r1 + ", " + r2 + "]";
            }
        }
        return "";
    }

    private static List<String> preprocessQueryResult(List<Object> resultSet) {
        return resultSet.stream().map(o -> {
            if (o == null) {
                return "[NULL]";
            } else {
                return o.toString();
            }
        }).sorted().collect(Collectors.toList());
    }
}
