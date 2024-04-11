package sqlancer.tidb.transaction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.tidb.gen.transaction.TiDBIsolationLevelGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;

public class TiDBTxTestExecutor {

    private static final int WAIT_THRESHOLD = 2;
    
    private final TiDBGlobalState globalState;
    private final List<Transaction> transactions;
    private final List<TxStatement> schedule;
    private final TiDBIsolationLevel isoLevel;

    public TiDBTxTestExecutor(TiDBGlobalState globalState, List<Transaction> transactions, List<TxStatement> schedule,
                              TiDBIsolationLevel isoLevel) {
        this.globalState = globalState;
        this.transactions = transactions;
        this.schedule = schedule;
        this.isoLevel = isoLevel;
    }

    public TxTestExecutionResult execute() throws SQLException {
        ExecutorService executor = Executors.newFixedThreadPool(transactions.size());
        Map<Transaction, Future<TxStatementExecutionResult>> blockedTxs = new HashMap<>();

        List<TxStatement> submittedStmts = new ArrayList<>();
        List<TxStatementExecutionResult> stmtExecutionResults = new ArrayList<>();

        TiDBIsolationLevelGenerator isoLevelGenerator = new TiDBIsolationLevelGenerator(isoLevel);
        for (Transaction tx : transactions) {
            TxSQLQueryAdapter isoQuery = new TxSQLQueryAdapter(isoLevelGenerator.getQuery());
            isoQuery.execute(tx);
        }
        
        while (submittedStmts.size() != schedule.size()) {
            for (TxStatement curStmt : schedule) {
                if (submittedStmts.contains(curStmt)) {
                    continue;
                }

                Transaction curTx = curStmt.getTransaction();
                if (blockedTxs.containsKey(curTx)) {
                    continue;
                }

                submittedStmts.add(curStmt);
                Future<TxStatementExecutionResult> stmtFuture = executor.submit(new TxStmtExecutor(curStmt));
                TxStatementExecutionResult stmtResult;
                try {
                    stmtResult = stmtFuture.get(WAIT_THRESHOLD, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    // add blocked stmt into execution order
                    TxStatementExecutionResult blockedStmtResult = new TxStatementExecutionResult(curStmt);
                    blockedStmtResult.setBlocked(true);
                    blockedTxs.put(curTx, stmtFuture);
                    stmtExecutionResults.add(blockedStmtResult);
                    continue;
                } catch (ExecutionException e) {
                    throw new RuntimeException("Transaction statement returning result exception: ", e);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Transaction statement interrupted exception: ", e);
                }

                if (stmtResult != null) {
                    stmtExecutionResults.add(stmtResult);

                    boolean hasResumedTxs = false;
                    Iterator<Transaction> txIterator = blockedTxs.keySet().iterator();
                    while (txIterator.hasNext()) {
                        Transaction blockedTx = txIterator.next();
                        Future<TxStatementExecutionResult> blockedStmtFuture = blockedTxs.get(blockedTx);
                        TxStatementExecutionResult blockedStmtResult = null;
                        try {
                            blockedStmtResult = blockedStmtFuture.get(WAIT_THRESHOLD, TimeUnit.SECONDS);
                        } catch (TimeoutException e) {
                            // ignore
                        } catch (ExecutionException e) {
                            throw new RuntimeException("Transaction blocked statement returning result exception: ", e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Transaction blocked statement interrupted exception: ", e);
                        }

                        if (blockedStmtResult != null) {
                            hasResumedTxs = true;
                            txIterator.remove();
                            stmtExecutionResults.add(blockedStmtResult);
                        }
                    }
                    if (hasResumedTxs) {
                        break;
                    }
                }
            }
        }
        
        TxTestExecutionResult txResult = new TxTestExecutionResult();
        txResult.setIsolationLevel(isoLevel);
        txResult.setStatementExecutionResults(stmtExecutionResults);
        txResult.setDbFinalStates(getDBState());
        return txResult;
    }

    class TxStmtExecutor implements Callable<TxStatementExecutionResult> {
        private TxStatement txStmt;

        public TxStmtExecutor(TxStatement txStmt) {
            this.txStmt = txStmt;
        }

        @Override
        public TxStatementExecutionResult call() {
            try {
                TxStatementExecutionResult stmtResult = new TxStatementExecutionResult(txStmt);
                if (txStmt.getType() == TxStatement.StatementType.SELECT ||
                        txStmt.getType() == TxStatement.StatementType.SELECT_FOR_UPDATE) {
                    SQLancerResultSet queryResult = null;
                    try {
                        queryResult = txStmt.getTxQueryAdapter().executeAndGet(txStmt.getTransaction());
                    } catch (SQLException e) {
                        stmtResult.setErrorInfo(e.getMessage());
                    }
                    stmtResult.setResult(QueryResultUtil.getQueryResult(queryResult));
                } else {
                    try {
                        txStmt.getTxQueryAdapter().execute(txStmt.getTransaction());
                    } catch (SQLException e) {
                        stmtResult.setErrorInfo(e.getMessage());
                    }
                }
                TxSQLQueryAdapter showSql = new TxSQLQueryAdapter("SHOW WARNINGS");
                SQLancerResultSet warningResult = showSql.executeAndGet(txStmt.getTransaction());
                stmtResult.setWarningInfo(QueryResultUtil.getQueryResult(warningResult));
                return stmtResult;
            } catch (SQLException e) {
                throw new RuntimeException("Transaction statement execution exception: ", e);
            }
        }
    }

    public Map<String, List<Object>> getDBState() throws SQLException { // This method is common in some cases? // It's only used here for now
        Map<String, List<Object>> dbStates = new HashMap<>();
        for (TiDBTable table : globalState.getSchema().getDatabaseTables()) {
            String query = "SELECT * FROM " + table.getName();
            SQLQueryAdapter sql = new SQLQueryAdapter(query);
            SQLancerResultSet tableState = sql.executeAndGet(globalState);
            dbStates.put(table.getName(), QueryResultUtil.getQueryResult(tableState));
        }
        return dbStates;
    }
}