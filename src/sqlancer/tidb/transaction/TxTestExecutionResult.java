package sqlancer.tidb.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;

public class TxTestExecutionResult {

    private TiDBIsolationLevel isolationLevel;
    private List<TxStatementExecutionResult> statementExecutionResults;
    private Map<String, List<Object>> dbFinalStates;

    public TxTestExecutionResult() {
        this.statementExecutionResults = new ArrayList<>();
        this.dbFinalStates = null;
    }

    public String dbFinalStateToString() {
        StringBuilder sb = new StringBuilder("[");
        for (Map.Entry<String, List<Object>> entry : dbFinalStates.entrySet()) {
            sb.append(entry.getKey()).append(": [");
            sb.append(entry.getValue().stream().map(o -> {
                if (o == null) {
                    return "NULL";
                } else {
                    return o.toString();
                }
            }).collect(Collectors.joining(", ")));
            sb.append("] ");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Schedule: ").append(statementExecutionResults).append("\n");
        sb.append("Isolation Level: ").append(isolationLevel).append("\n");
        sb.append("Query Results:\n");
        for (TxStatementExecutionResult statementExecutionResult : statementExecutionResults) {
            sb.append("\t").append(statementExecutionResult.getStatement().getStmtId())
                    .append(": ").append(statementExecutionResult.getResultAsString()).append("\n");
        }
        sb.append("DB Final State: ").append(dbFinalStateToString()).append("\n");
        return sb.toString();
    }

    public TiDBIsolationLevel getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(TiDBIsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public List<TxStatementExecutionResult> getStatementExecutionResults() {
        return statementExecutionResults;
    }

    public void setStatementExecutionResults(List<TxStatementExecutionResult> statementExecutionResults) {
        this.statementExecutionResults = statementExecutionResults;
    }

    public Map<String, List<Object>> getDbFinalStates() {
        return dbFinalStates;
    }

    public void setDbFinalStates(Map<String, List<Object>> dbFinalStates) {
        this.dbFinalStates = dbFinalStates;
    }
}
