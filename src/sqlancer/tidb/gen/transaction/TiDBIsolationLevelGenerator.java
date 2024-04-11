package sqlancer.tidb.gen.transaction;

import java.sql.SQLException;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;

public class TiDBIsolationLevelGenerator {

    private final TiDBIsolationLevel isolationLevel;

    public TiDBIsolationLevelGenerator(TiDBIsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public SQLQueryAdapter getQuery() throws SQLException {
        String sql = "SET SESSION TRANSACTION ISOLATION LEVEL " + isolationLevel.getName();
        return new SQLQueryAdapter(sql);
    }
}
