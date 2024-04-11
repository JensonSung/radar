package sqlancer.tidb.transaction;

import java.sql.SQLException;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;

public class TxSQLQueryAdapter extends SQLQueryAdapter {

    public TxSQLQueryAdapter(String query) {
        this(query, new ExpectedErrors());
    }

    public TxSQLQueryAdapter(String query, boolean couldAffectSchema) {
        this(query, new ExpectedErrors(), couldAffectSchema);
    }

    public TxSQLQueryAdapter(String query, ExpectedErrors expectedErrors) {
        this(query, expectedErrors, false);
    }
    
    public TxSQLQueryAdapter(String query, ExpectedErrors expectedErrors, boolean couldAffectSchema) {
        super(query, expectedErrors, couldAffectSchema);
    }

    public TxSQLQueryAdapter(SQLQueryAdapter queryAdapter) {
        this(queryAdapter.getQueryString(), queryAdapter.getExpectedErrors(), queryAdapter.couldAffectSchema());
    }

    public boolean execute(Transaction transaction, String... fills) throws SQLException {
            return internalExecute(transaction.getConnection(), true, fills);
    }
    
    public SQLancerResultSet executeAndGet(Transaction transaction, String... fills)
            throws SQLException {
            return internalExecuteAndGet(transaction.getConnection(), true, fills);
    }

}
