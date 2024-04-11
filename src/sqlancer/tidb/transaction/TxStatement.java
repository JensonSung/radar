package sqlancer.tidb.transaction;

import sqlancer.common.query.SQLQueryAdapter;

public class TxStatement {
    private Transaction transaction;
    private TxSQLQueryAdapter txQueryAdapter;
    private StatementType type;

    public enum StatementType {
        BEGIN, COMMIT, ROLLBACK,
        SELECT, SELECT_FOR_UPDATE,
        UPDATE, DELETE, INSERT, REPLACE,
        SET,
        UNKNOWN
    }

    public TxStatement(Transaction transaction, TxSQLQueryAdapter txQueryAdapter) {
        this.transaction = transaction;
        this.txQueryAdapter = txQueryAdapter;
        setStatementType();
    }
    
    public TxStatement(Transaction transaction, SQLQueryAdapter queryAdapter) {
        this(transaction, new TxSQLQueryAdapter(queryAdapter));
    }

    private void setStatementType() {
        String stmt = txQueryAdapter.getQueryString().replace(";", "").toUpperCase();
        StatementType realType = StatementType.valueOf(stmt.split(" ")[0]);
        if (realType == StatementType.SELECT) {
            int forIdx = stmt.indexOf("FOR ");
            if (forIdx != -1) {
                String postfix = stmt.substring(forIdx);
                // not implement FOR SHARE, since TiDB does not support FOR SHARE
                if (postfix.equals("FOR UPDATE")) {
                    realType = StatementType.SELECT_FOR_UPDATE;
                } else {
                    throw new RuntimeException("Invalid postfix: " + txQueryAdapter.getQueryString());
                }
            }
        }
        type = realType;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public TxSQLQueryAdapter getTxQueryAdapter() {
        return txQueryAdapter;
    }

    public StatementType getType() {
        return type;
    }

    public String getStmtId() {
        return String.format("%d-%d", transaction.getId(), transaction.getStatements().indexOf(this));
    }
    
    @Override
    public String toString() {
        return String.format("%d-%d: %s", transaction.getId(), transaction.getStatements().indexOf(this),
                txQueryAdapter.getQueryString());
    }
}
