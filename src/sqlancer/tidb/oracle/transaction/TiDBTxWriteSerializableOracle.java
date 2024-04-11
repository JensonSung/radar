package sqlancer.tidb.oracle.transaction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.oracle.TxBase;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;
import sqlancer.tidb.transaction.TiDBTxTestExecutor;
import sqlancer.tidb.transaction.Transaction;
import sqlancer.tidb.transaction.TxSQLQueryAdapter;
import sqlancer.tidb.transaction.TxStatement;
import sqlancer.tidb.transaction.TxStatementExecutionResult;
import sqlancer.tidb.transaction.TxTestExecutionResult;
import sqlancer.tidb.transaction.TxTestGenerator;

public class TiDBTxWriteSerializableOracle extends TxBase<TiDBGlobalState> {

    public TiDBTxWriteSerializableOracle(TiDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        logger.writeCurrent("\n================= Generate new transaction list =================");
        TxTestGenerator txTestGenerator = new TxTestGenerator(state);
        List<Transaction> transactions = txTestGenerator.generateTransactions();
        for (Transaction tx : transactions) {
            logger.writeCurrent(tx.toString());
        }
        List<List<TxStatement>> schedules = txTestGenerator.genSchedules(transactions);

        try {
            for (List<TxStatement> schedule : schedules) {
                logger.writeCurrent("Input schedule: " + schedule.stream().map(TxStatement::getStmtId).
                        collect(Collectors.joining(", ", "[", "]")));
                TiDBIsolationLevel isoLevel = Randomly.fromOptions(TiDBIsolationLevel.values());
                logger.writeCurrent("Isolation level: " + isoLevel);
                TiDBTxTestExecutor testExecutor = new TiDBTxTestExecutor(state, transactions, schedule, isoLevel);

                TxTestExecutionResult testResult = testExecutor.execute();
                reproduceDatabase(state.getState().getStatements());
                
                List<TxStatement> oracleSchedule = genOracleSchedule(testResult);
                TiDBTxTestExecutor oracleExecutor = new TiDBTxTestExecutor(state, transactions, oracleSchedule, isoLevel);
                TxTestExecutionResult oracleResult = oracleExecutor.execute();
                reproduceDatabase(state.getState().getStatements());
                
                String compareResultInfo = compareFinalDBState(testResult, oracleResult);
                if (!compareResultInfo.equals("")) {
                    state.getState().getLocalState().log("============Bug Report============");
                    for (Transaction tx : transactions) {
                        state.getState().getLocalState().log(tx.toString());
                    }
                    state.getState().getLocalState().log("Input schedule: " + schedule.stream().map(TxStatement::getStmtId).
                            collect(Collectors.joining(", ", "[", "]")));
                    state.getState().getLocalState().log(compareResultInfo);
                    state.getState().getLocalState().log("Execution Result:");
                    state.getState().getLocalState().log(testResult.toString());
                    state.getState().getLocalState().log("Oracle Result:");
                    state.getState().getLocalState().log(oracleResult.toString());
                    throw new AssertionError("Transaction execution mismatches its oracles");
                } else {
                    state.getLogger().writeCurrent("============Is Same============");
                }
            }
        } finally {
            for (Transaction tx : transactions) {
                tx.closeConnection();
            }
        }
    }

    public List<TxStatement> genOracleSchedule(TxTestExecutionResult testResult) {
        List<TxStatement> oracleSchedule = new ArrayList<>();
        List<Integer> deadlockTxIds = new ArrayList<>();
        for (TxStatementExecutionResult stmtResult : testResult.getStatementExecutionResults()) {
            Transaction stmtTx = stmtResult.getStatement().getTransaction();
            if (!stmtResult.isBlocked()) {
                if (stmtResult.reportDeadlock()) {
                    for (TxStatement stmt : stmtTx.getStatements()) {
                        oracleSchedule.add(stmt);
                        if (stmtResult.getStatement().equals(stmt)) {
                            break;
                        }
                    }
                    // If tx reports deadlock, tx should be rollbacked.
                    TxStatement rollbackStmt = new TxStatement(stmtTx, new TxSQLQueryAdapter("ROLLBACK"));
                    oracleSchedule.add(rollbackStmt);
                    deadlockTxIds.add(stmtTx.getId());
                    continue;
                }
                if (deadlockTxIds.contains(stmtTx.getId())) {
                    // We assume that each stmt after deadlock is executed as an auto-committed transaction.
                    oracleSchedule.add(stmtResult.getStatement());
                } else if (stmtResult.getStatement().getType() == TxStatement.StatementType.COMMIT
                        || stmtResult.getStatement().getType() == TxStatement.StatementType.ROLLBACK) {
                    oracleSchedule.addAll(stmtTx.getStatements());
                }
            }
        }
        return oracleSchedule;
    }
}
