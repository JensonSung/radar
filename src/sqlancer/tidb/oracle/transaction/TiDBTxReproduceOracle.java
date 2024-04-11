package sqlancer.tidb.oracle.transaction;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import sqlancer.common.oracle.TxBase;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.TiDBSchema.TiDBTable;
import sqlancer.tidb.transaction.TxTestExecutionResult;
import sqlancer.tidb.transaction.TiDBTxTestExecutor;
import sqlancer.tidb.transaction.Transaction;
import sqlancer.tidb.transaction.TxSQLQueryAdapter;
import sqlancer.tidb.transaction.TxStatement;
import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;

public class TiDBTxReproduceOracle extends TxBase<TiDBGlobalState> {

    public TiDBTxReproduceOracle(TiDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        // delete tables that are not the target
        dropTables();

        Scanner scanner;
        try {
            File caseFile = new File(options.getCaseFile());
            scanner = new Scanner(caseFile);
            logger.writeCurrent("Read database and transaction from file: " + options.getCaseFile());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Read case from file failed: ", e);
        }
        String tableName = options.getCaseTableName();
        List<Query<?>> dbInitQueries = prepareTableFromScanner(scanner, tableName);
        reproduceDatabase(dbInitQueries);
        // String isolationAlias = scanner.nextLine();
        // TiDBIsolationLevel tiDBIsolationLevel = TiDBIsolationLevel.getFromAlias(isolationAlias);
        // String temp = scanner.nextLine();
        List<Transaction> transactions = new ArrayList<>();
        for (int i = 0; i < state.getDbmsSpecificOptions().getNrTransactions(); i++) {
            transactions.add(readTransactionFromScanner(scanner));
        }
        String scheduleStr = readOrderFromScanner(scanner);
        List<TxStatement> schedule = checkOrder(scheduleStr, transactions);
        boolean detectBug = false;
        try {
            for (TiDBIsolationLevel level : TiDBIsolationLevel.values()) {
                TiDBTxTestExecutor testExecutor = new TiDBTxTestExecutor(state, transactions, schedule, level);
                TxTestExecutionResult testResult = testExecutor.execute();
                reproduceDatabase(dbInitQueries);
                TiDBTxInfer infer = new TiDBTxInfer(state, transactions, testResult, level);
                TxTestExecutionResult oracleResult = infer.inferOracle();
                String compareResultInfo = compareAllResults(testResult, oracleResult);
                if (compareResultInfo.equals("")) {
                    state.getLogger().writeCurrent("============Is Same============");
                    state.getLogger().writeCurrent("Execution Result:");
                    state.getLogger().writeCurrent(testResult.toString());
                    state.getLogger().writeCurrent("Oracle Result:");
                    state.getLogger().writeCurrent(oracleResult.toString());
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
                    detectBug = true;
                }
                reproduceDatabase(dbInitQueries);
            }
            if (detectBug) {
                throw new AssertionError("Transaction execution mismatches its oracle");
            }
        } finally {
            for (Transaction tx : transactions) {
                tx.closeConnection();
            }
        }
        System.exit(0);
    }

    private void dropTables() throws SQLException {
        // delete tables that are not the target
        for (TiDBTable t : state.getSchema().getDatabaseTables()) {
            SQLQueryAdapter sql;
            if (t.isView()) {
                sql = new SQLQueryAdapter(String.format("DROP VIEW IF EXISTS %s", t.getName()), true);
            } else {
                sql = new SQLQueryAdapter(String.format("DROP TABLE IF EXISTS %s", t.getName()), true);
            }
            sql.execute(state);
        }
        try {
            state.updateSchema();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    private List<Query<?>> prepareTableFromScanner(Scanner input, String tableName) throws SQLException {
        List<Query<?>> dbInitQueries = new ArrayList<>();
        SQLQueryAdapter dropTable = new SQLQueryAdapter("DROP TABLE IF EXISTS " + tableName);
        dropTable.execute(state);
        dbInitQueries.add(dropTable);
        String sql;
        do {
            sql = input.nextLine();
            if (sql.equals("")) break;
            SQLQueryAdapter queryAdapter;
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            TiDBErrors.addInsertErrors(errors);
            if (sql.contains("CREATE") || sql.contains("ALTER TABLE")) {
                queryAdapter = new SQLQueryAdapter(sql, errors, true);
            } else {
                queryAdapter = new SQLQueryAdapter(sql, errors);
            }
            try {
                queryAdapter.execute(state, false);
                dbInitQueries.add(queryAdapter);
            } catch (Exception e) {
                throw new SQLException(e);
            }
        } while (true);
        try {
            state.updateSchema();
        } catch (Exception e) {
            throw new SQLException(e);
        }
        return dbInitQueries;
    }

    private Transaction readTransactionFromScanner(Scanner input) throws SQLException {
        Transaction transaction = new Transaction(state.createConnection());
        List<TxStatement> statementList = new ArrayList<>();
        String txId = input.nextLine();
        transaction.setId(Integer.parseInt(txId));
        String sql;
        do {
            if (!input.hasNext()) break;
            sql = input.nextLine();
            if (sql.equals("") || sql.equals("END")) break;
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            TiDBErrors.addInsertErrors(errors);
            TiDBErrors.addExpressionHavingErrors(errors);
            errors.add("Deadlock found");
            TxSQLQueryAdapter txStatement = new TxSQLQueryAdapter(sql, errors);
            TxStatement cell = new TxStatement(transaction, txStatement);
            statementList.add(cell);
        } while (true);
        transaction.setStatements(statementList);
        return transaction;
    }

    private String readOrderFromScanner(Scanner input) {
        do {
            if (!input.hasNext()) break;
            String scheduleStr = input.nextLine();
            if (scheduleStr.equals("")) continue;
            if (scheduleStr.equals("END")) break;
            return scheduleStr;
        } while (true);
        return "";
    }

    private List<TxStatement> checkOrder(String scheduleStr, List<Transaction> transactions) {
        List<TxStatement> schedule = new ArrayList<>();
        Map<Integer, Transaction> txIdMap = new HashMap<>();
        Map<Integer, Integer> txStmtIndex = new HashMap<>();
        String[] scheduleStrArray = scheduleStr.split("-");
        int allStmtsLength = 0;
        for (Transaction transaction : transactions) {
            allStmtsLength += transaction.getStatements().size();
            txIdMap.put(transaction.getId(), transaction);
            txStmtIndex.put(transaction.getId(), 0);
        }
        if (scheduleStrArray.length != allStmtsLength) {
            throw new RuntimeException("Invalid Schedule");
        }

        for (String txIdStr : scheduleStrArray) {
            int txId = Integer.parseInt(txIdStr);
            if (txIdMap.containsKey(txId)){
                Transaction tx = txIdMap.get(txId);
                int stmtIdx = txStmtIndex.get(txId);
                schedule.add(tx.getStatements().get(stmtIdx++));
                txStmtIndex.replace(txId, stmtIdx);
            } else {
                throw new RuntimeException("Invalid Schedule");
            }
        }
        return schedule;
    }
}
