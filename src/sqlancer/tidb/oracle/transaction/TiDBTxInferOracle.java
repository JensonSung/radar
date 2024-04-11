package sqlancer.tidb.oracle.transaction;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.oracle.TxBase;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.transaction.TxTestExecutionResult;
import sqlancer.tidb.transaction.TiDBTxTestExecutor;
import sqlancer.tidb.transaction.Transaction;
import sqlancer.tidb.transaction.TxStatement;
import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;
import sqlancer.tidb.transaction.TxTestGenerator;

public class TiDBTxInferOracle extends TxBase<TiDBGlobalState> {

    public TiDBTxInferOracle(TiDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        // delete view, since we do not support views
        dropViewTable();

        logger.writeCurrent("\n================= Generate new transaction list =================");
        TxTestGenerator txTestGenerator = new TxTestGenerator(state);
        List<Transaction> transactions = txTestGenerator.generateTransactions();
        for (Transaction tx : transactions) {
            logger.writeCurrent(tx.toString());
        }
        List<List<TxStatement>> schedules = txTestGenerator.genSchedules(transactions);

        try {
            for (List<TxStatement> schedule : schedules) {
                logger.writeCurrent("Input schedule: " + schedule.stream().map(o -> o.getStmtId()).
                        collect(Collectors.joining(", ", "[", "]")));
                TiDBIsolationLevel isoLevel = Randomly.fromOptions(TiDBIsolationLevel.values());

                TiDBTxTestExecutor testExecutor = new TiDBTxTestExecutor(state, transactions, schedule, isoLevel);
                TxTestExecutionResult testResult = testExecutor.execute();

                reproduceDatabase(state.getState().getStatements());
                dropViewTable();
                TiDBTxInfer infer = new TiDBTxInfer(state, transactions, testResult, isoLevel);
                TxTestExecutionResult oracleResult = infer.inferOracle();
                String compareResultInfo = compareAllResults(testResult, oracleResult);
                if (compareResultInfo.equals("")) {
                    state.getLogger().writeCurrent("============Is Same============");
                } else {
                    state.getState().getLocalState().log("============Bug Report============");
                    for (Transaction tx : transactions) {
                        state.getState().getLocalState().log(tx.toString());
                    }
                    state.getState().getLocalState().log("Input schedule: " + schedule.stream().map(o -> o.getStmtId()).
                            collect(Collectors.joining(", ", "[", "]")));
                    state.getState().getLocalState().log(compareResultInfo);
                    state.getState().getLocalState().log("Execution Result:");
                    state.getState().getLocalState().log(testResult.toString());
                    state.getState().getLocalState().log("Oracle Result:");
                    state.getState().getLocalState().log(oracleResult.toString());
                    throw new AssertionError("Transaction execution mismatches its oracle");
                }

                reproduceDatabase(state.getState().getStatements());
                dropViewTable();
            }
        } finally {
            for (Transaction tx : transactions) {
                tx.closeConnection();
            }
        }
    }

    private void dropViewTable() throws SQLException {
        // delete view, since we do not support views
        for (TiDBTable t : state.getSchema().getDatabaseTables()) {
            if (t.isView()) {
                SQLQueryAdapter sql = new SQLQueryAdapter(String.format("DROP VIEW IF EXISTS %s", t.getName()),
                        true);
                sql.execute(state);
            }
        }
        try {
            state.updateSchema();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
