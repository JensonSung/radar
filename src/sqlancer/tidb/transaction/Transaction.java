package sqlancer.tidb.transaction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import sqlancer.SQLConnection;

public class Transaction {

    private static AtomicInteger idCounter = new AtomicInteger(1);
    private int id;
    private List<TxStatement> statements;
    private SQLConnection connection;

    public Transaction(SQLConnection connection) {
        this.id = idCounter.getAndIncrement();
        this.statements = new ArrayList<>();
        this.connection = connection;
    }

    public void addStatement(TxStatement stmt) {
        statements.add(stmt);
    }
    
    public void closeConnection() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    public static AtomicInteger getIdCounter() {
        return idCounter;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public List<TxStatement> getStatements() {
        return statements;
    }

    public void setStatements(List<TxStatement> statements) {
        this.statements = statements;
    }

    public SQLConnection getConnection() {
        return connection;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(" -- Transaction %d, with statements:\n", id));
        if (statements != null) {
            for (TxStatement stmt : statements) {
                sb.append(stmt.getTxQueryAdapter().getQueryString()).append("\n");
            }
        }
        return sb.toString();
    }
}
